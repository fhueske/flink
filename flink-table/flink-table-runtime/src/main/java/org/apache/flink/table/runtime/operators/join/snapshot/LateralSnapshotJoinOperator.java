/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * Stream operator implementing the {@code LATERAL SNAPSHOT} processing-time temporal table join.
 *
 * <p>The operator runs in two phases, {@code LOAD} and {@code JOIN}:
 *
 * <ul>
 *   <li>{@code LOAD}: build-side (input2 / right) records are applied directly to a per-key
 *       multi-set in {@code buildTableState}. Probe-side (input1 / left) records are buffered in
 *       {@code probeBuffer} until the configured flip point is reached on the build-side watermark,
 *       at which point a per-key event-time timer drains the buffered probes and joins them with
 *       the materialized build-side state.
 *   <li>{@code JOIN}: probe-side records are joined immediately with the current build-side state.
 *       Build-side updates are buffered in {@code buildChangeBuffer} and applied lazily on next
 *       per-key access once the build-side watermark has advanced past the buffer's tag. This
 *       preserves atomic update visibility across {@code -U}/{@code +U} pairs.
 * </ul>
 *
 * <p>Watermark forwarding rules:
 *
 * <ul>
 *   <li>Build-side watermarks are never forwarded downstream.
 *   <li>Probe-side watermarks are held back during {@code LOAD} and forwarded during {@code JOIN}.
 * </ul>
 *
 * <p>The flip from {@code LOAD} to {@code JOIN} is triggered by either:
 *
 * <ul>
 *   <li>the build-side watermark reaching {@code loadCompletedTime} (event-time gate), or
 *   <li>the {@code loadCompletedIdleTimeoutMs} processing-time timer firing without any build-side
 *       watermark advance.
 * </ul>
 *
 * <p>State TTL is implemented manually with keyed processing-time timers (matching the semantics of
 * Flink's standard {@code StateTtlConfig}) and is active only during {@code JOIN}.
 */
@Internal
public class LateralSnapshotJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>, Triggerable<RowData, String> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(LateralSnapshotJoinOperator.class);

    private static final String OPERATOR_PHASE_STATE_NAME = "lateral-snapshot-phase";
    private static final String OPERATOR_FLIP_TIME_STATE_NAME = "lateral-snapshot-flip-time";
    private static final String BUILD_TABLE_STATE_NAME = "build-table";
    private static final String BUILD_CHANGE_BUFFER_STATE_NAME = "build-change-buffer";
    private static final String BUFFERED_AT_WM_STATE_NAME = "buffered-at-wm";
    private static final String PROBE_BUFFER_STATE_NAME = "probe-buffer";
    private static final String TTL_EXPIRY_STATE_NAME = "ttl-expiry";

    private static final String TIMER_SERVICE_NAME = "lateral-snapshot-timers";

    @VisibleForTesting static final String NS_FLUSH = "flush";
    @VisibleForTesting static final String NS_TTL = "ttl";

    /**
     * Event-time timestamp at which the per-key {@code probeBuffer} flush timer is registered. Any
     * non-{@code MIN_VALUE} watermark advance fires it.
     */
    @VisibleForTesting static final long FLUSH_TIMER_TS = 1L;

    /** Two-phase state machine. */
    public enum Phase {
        LOAD,
        JOIN
    }

    // -------------------------- ctor args --------------------------

    /** {@code true} for {@code LEFT OUTER JOIN}, {@code false} for {@code INNER JOIN}. */
    private final boolean isLeftOuterJoin;

    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;
    private final GeneratedJoinCondition generatedJoinCondition;

    /**
     * Per-equi-key flag indicating whether rows with a NULL in that key position must be filtered
     * before the join condition runs (SQL semantics: {@code NULL = NULL} is not true). Mirrors
     * {@code JoinSpec#getFilterNulls()} from the planner.
     */
    private final boolean[] filterNullKeys;

    /**
     * Wall-clock timestamp (millis since epoch) at which the build-side watermark must arrive for
     * the operator to flip from {@code LOAD} to {@code JOIN}. Resolved at planning time, so a
     * compile-time-default condition becomes a concrete value here. {@code null} when the operator
     * relies solely on the idle-timeout fallback.
     */
    @Nullable private final Long loadCompletedTime;

    /**
     * Processing-time idle timeout (millis) on build-side watermarks. When configured, the operator
     * flips to {@code JOIN} if no build-side watermark advance is seen for this duration.
     */
    @Nullable private final Long loadCompletedIdleTimeoutMs;

    /** Minimum state TTL (millis) applied to build-side keyed state during {@code JOIN}. */
    @Nullable private final Long stateTtlMs;

    // -------------------------- transient runtime --------------------------

    private transient Phase phase;

    /**
     * Processing-time wall clock at which the operator transitioned from {@link Phase#LOAD} to
     * {@link Phase#JOIN}. {@code null} while still in {@code LOAD}. Used by the TTL handler to
     * grant a grace period of {@code stateTtlMs} after the flip before any build-only key becomes
     * eligible for eviction (see {@link #onProcessingTime}). Persisted in operator union-list state
     * so it survives rescaling.
     */
    @Nullable private transient Long flipProcTime;

    /** Highest build-side watermark observed; not persisted. */
    private transient long latestBuildSideWm;

    /** Latest probe-side watermark observed during LOAD; forwarded on flip. */
    private transient long lastProbeWm;

    private transient JoinConditionWithNullFilters joinCondition;
    private transient GenericRowData nullPaddedBuild;
    private transient TimestampedCollector<RowData> collector;

    private transient InternalTimerService<String> timerService;

    /** Non-keyed processing-time idle-flip timer. */
    @Nullable private transient ScheduledFuture<?> idleFlipTimer;

    private transient RowDataSerializer rightSerializer;
    private transient RowDataSerializer leftSerializer;

    // -------------------------- keyed state --------------------------

    /** Build-side multi-set: build-row → reference count. Row kind is normalized to INSERT. */
    private transient MapState<RowData, Long> buildTableState;

    /** Build-side changes deferred for lazy application during JOIN. */
    private transient ListState<RowData> buildChangeBuffer;

    /**
     * Build-side watermark observed when the first change was added to {@code buildChangeBuffer};
     * {@code null} when the buffer is empty.
     */
    private transient ValueState<Long> bufferedAtWmState;

    /** Probe-side records buffered during LOAD. */
    private transient ListState<RowData> probeBuffer;

    /** Most recently registered TTL timer deadline; used to filter stale fires. */
    private transient ValueState<Long> ttlExpiryState;

    // -------------------------- operator state --------------------------

    private transient ListState<String> operatorPhaseState;

    /** Persisted flip processing-time (one entry per parallel subtask in JOIN). */
    private transient ListState<Long> operatorFlipTimeState;

    public LateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            @Nullable Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        // At least one flip trigger must be configured; otherwise the operator would buffer
        // probes forever in LOAD with no path to JOIN.
        Preconditions.checkArgument(
                loadCompletedTime != null || loadCompletedIdleTimeoutMs != null,
                "LateralSnapshotJoinOperator requires loadCompletedTime or "
                        + "loadCompletedIdleTimeoutMs to be configured.");
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.filterNullKeys = Preconditions.checkNotNull(filterNullKeys);
        this.loadCompletedTime = loadCompletedTime;
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        this.stateTtlMs = stateTtlMs;
    }

    // -------------------------- lifecycle --------------------------

    @Override
    public boolean useInterruptibleTimers(ReadableConfig config) {
        return true;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Operator state only — keyed state and timer services are initialized in open() since
        // the keyed-state runtime context isn't fully wired until then. Mirrors the pattern used
        // by TemporalRowTimeJoinOperator.
        operatorPhaseState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        OPERATOR_PHASE_STATE_NAME, StringSerializer.INSTANCE));
        operatorFlipTimeState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        OPERATOR_FLIP_TIME_STATE_NAME, Types.LONG));

        // any LOAD entry → LOAD; empty (fresh start) → LOAD; else JOIN
        boolean anyEntry = false;
        boolean anyLoad = false;
        for (String entry : operatorPhaseState.get()) {
            anyEntry = true;
            if (Phase.LOAD.name().equals(entry)) {
                anyLoad = true;
                break;
            }
        }
        phase = (!anyEntry || anyLoad) ? Phase.LOAD : Phase.JOIN;

        // Recover flipProcTime when restored into JOIN. We pick the maximum across all
        // contributing subtasks — i.e. the most recent flip — which anchors the grace window
        // on the moment the entire (parallel) operator had finished flipping. Earlier
        // per-subtask flip times are subsumed by the latest one.
        flipProcTime = null;
        if (phase == Phase.JOIN) {
            long maxFlipTime = Long.MIN_VALUE;
            boolean anyFlipTime = false;
            for (Long t : operatorFlipTimeState.get()) {
                if (t != null) {
                    maxFlipTime = Math.max(maxFlipTime, t);
                    anyFlipTime = true;
                }
            }
            if (anyFlipTime) {
                flipProcTime = maxFlipTime;
            }
        }

        latestBuildSideWm = Long.MIN_VALUE;
        lastProbeWm = Long.MIN_VALUE;
    }

    @Override
    public void open() throws Exception {
        super.open();

        buildTableState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        BUILD_TABLE_STATE_NAME, rightType, Types.LONG));
        buildChangeBuffer =
                getRuntimeContext()
                        .getListState(
                                new ListStateDescriptor<>(
                                        BUILD_CHANGE_BUFFER_STATE_NAME, rightType));
        bufferedAtWmState =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(BUFFERED_AT_WM_STATE_NAME, Types.LONG));
        probeBuffer =
                getRuntimeContext()
                        .getListState(new ListStateDescriptor<>(PROBE_BUFFER_STATE_NAME, leftType));
        ttlExpiryState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(TTL_EXPIRY_STATE_NAME, Types.LONG));

        // Wrap the codegen'd condition with a null-key filter so SQL semantics are honored for
        // equi-keys whose values may be NULL. The codegen body covers the non-equi part only;
        // equi-keys are enforced via partitioning, but `NULL = NULL` is not a match in SQL, so
        // null-keyed rows must be dropped here regardless of how they were partitioned.
        final JoinCondition rawCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        joinCondition = new JoinConditionWithNullFilters(rawCondition, filterNullKeys, this);
        joinCondition.setRuntimeContext(getRuntimeContext());
        joinCondition.open(DefaultOpenContext.INSTANCE);

        nullPaddedBuild = new GenericRowData(rightType.toRowSize());
        collector = new TimestampedCollector<>(output);

        rightSerializer = rightType.toRowSerializer();
        leftSerializer = leftType.toRowSerializer();

        timerService = getInternalTimerService(TIMER_SERVICE_NAME, StringSerializer.INSTANCE, this);

        // Mark the build-side input (index 1) as permanently idle in the inherited
        // combinedWatermark accounting. This operator never forwards build-side WMs nor
        // build-side idle status: it absorbs both. By keeping partial[1] idle we ensure
        //   - the combined watermark = partial[0] (probe-side) and never regresses to
        //     build-side WMs (which we never advance);
        //   - any processWatermarkStatus() invocation on input 1 (which AbstractStreamOperator
        //     dispatches via FINAL methods we cannot override per-index) sees an already-idle
        //     partial and emits nothing spurious downstream.
        combinedWatermark.updateStatus(1, true);

        if (phase == Phase.LOAD && loadCompletedIdleTimeoutMs != null) {
            scheduleIdleFlipTimer();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        operatorPhaseState.update(Collections.singletonList(phase.name()));
        operatorFlipTimeState.update(
                flipProcTime == null
                        ? Collections.emptyList()
                        : Collections.singletonList(flipProcTime));
    }

    @Override
    public void close() throws Exception {
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
        if (joinCondition != null) {
            joinCondition.close();
        }
        super.close();
    }

    // -------------------------- elements --------------------------

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData probe = element.getValue();
        if (phase == Phase.LOAD) {
            probeBuffer.add(probe);
            timerService.registerEventTimeTimer(NS_FLUSH, FLUSH_TIMER_TS);
        } else {
            drainBufferIfPending();
            joinProbeRow(probe);
            refreshTtl();
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData build = element.getValue();
        Long bufferedAt = bufferedAtWmState.value();
        if (phase == Phase.LOAD) {
            // Recovery from a JOIN-phase snapshot rescaled into LOAD: drain the buffer first.
            if (bufferedAt != null) {
                drainBuffer();
            }
            applyBuildChange(build);
        } else {
            // drainBufferIfPending inlined to avoid a second read of bufferedAtWmState.
            if (bufferedAt != null && latestBuildSideWm > bufferedAt) {
                drainBuffer();
                bufferedAt = null;
            }
            buildChangeBuffer.add(build);
            if (bufferedAt == null) {
                bufferedAtWmState.update(latestBuildSideWm);
            }
            refreshTtl();
        }
    }

    // -------------------------- watermarks --------------------------

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        // Probe-side watermark.
        if (phase == Phase.LOAD) {
            lastProbeWm = Math.max(lastProbeWm, mark.getTimestamp());
            // do not advance timer service, do not forward
        } else {
            // Route through the framework's index-aware processWatermark so combinedWatermark[0]
            // stays in sync with what we emit. With partial[1] kept idle (see open()), the
            // combined value reduces to partial[0] = mark, so the framework emits exactly this
            // watermark downstream and advances the keyed timer service to the same value. Keeping
            // combinedWatermark[0] up to date is what prevents a later processWatermarkStatus on
            // input 0 from emitting a smaller spurious WM via the inherited status path.
            super.processWatermark1(mark);
        }
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        // Build-side watermark: NEVER forwarded; never advances the timer service.
        long ts = mark.getTimestamp();
        latestBuildSideWm = Math.max(latestBuildSideWm, ts);
        if (phase == Phase.LOAD) {
            if (loadCompletedTime != null && latestBuildSideWm >= loadCompletedTime) {
                flip();
            } else if (loadCompletedIdleTimeoutMs != null) {
                rescheduleIdleFlipTimer();
            }
        }
    }

    @Override
    protected void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
            throws Exception {
        if (index == 1) {
            // Build-side idle status is absorbed entirely. partial[1] is initialized idle in
            // open() and stays that way regardless of source-side toggles, so combined accounting
            // is always driven by the probe side alone.
            return;
        }
        if (phase == Phase.LOAD) {
            // FLIP: nothing is emitted downstream during LOAD — neither watermarks nor status.
            // Keep partial[0]'s state untouched; we re-emit on flip.
            return;
        }
        super.processWatermarkStatus(watermarkStatus, index);
    }

    // -------------------------- timers --------------------------

    @Override
    public void onEventTime(InternalTimer<RowData, String> timer) throws Exception {
        String ns = timer.getNamespace();
        if (NS_FLUSH.equals(ns)) {
            // 1. Drain any buffered build changes for this key first (so probes see the latest
            //    build state when joined).
            drainBufferIfPending();

            // 2. Materialize and clear the probe buffer, then join each row.
            List<RowData> probes = new ArrayList<>();
            for (RowData p : probeBuffer.get()) {
                probes.add(leftSerializer.copy(p));
            }
            probeBuffer.clear();
            for (RowData p : probes) {
                joinProbeRow(p);
            }

            // 3. The post-flip flush counts as access — refresh TTL.
            refreshTtl();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, String> timer) throws Exception {
        // TTL timers run on processing time so semantics match Flink's standard StateTtlConfig.
        if (!NS_TTL.equals(timer.getNamespace())) {
            return;
        }
        if (stateTtlMs == null) {
            // Defensive: stateTtlMs is configured at construction; refreshTtl never arms a timer
            // when it's null, so this branch is unreachable. Guard prevents the reschedule path
            // below from looping at deadline=now if the invariant ever changes.
            return;
        }
        Long deadline = ttlExpiryState.value();
        if (deadline == null || timer.getTimestamp() != deadline) {
            return; // stale timer fire
        }
        // Grace-period rescheduling: a TTL fire is deferred when
        //   (a) we're still in LOAD (no flip yet — keep build state alive while loading), or
        //   (b) we've flipped but less than stateTtlMs has elapsed since the flip.
        // Without (b), build-only keys loaded long before the flip would be evicted on the
        // first TTL fire after the flip — even though no JOIN-phase access ever had a chance
        // to refresh the deadline. With (b), every key gets at least one full stateTtlMs of
        // post-flip "grace" before becoming eligible for eviction.
        long now = getProcessingTimeService().getCurrentProcessingTime();
        if (phase == Phase.LOAD || (flipProcTime != null && now < flipProcTime + stateTtlMs)) {
            long newDeadline = phase == Phase.LOAD ? now + stateTtlMs : flipProcTime + stateTtlMs;
            timerService.registerProcessingTimeTimer(NS_TTL, newDeadline);
            ttlExpiryState.update(newDeadline);
            return;
        }
        buildTableState.clear();
        buildChangeBuffer.clear();
        bufferedAtWmState.clear();
        ttlExpiryState.clear();
    }

    private void scheduleIdleFlipTimer() {
        long deadline =
                getProcessingTimeService().getCurrentProcessingTime() + loadCompletedIdleTimeoutMs;
        idleFlipTimer = getProcessingTimeService().registerTimer(deadline, t -> handleIdleFlip());
    }

    private void rescheduleIdleFlipTimer() {
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
        scheduleIdleFlipTimer();
    }

    private void handleIdleFlip() throws Exception {
        if (phase == Phase.LOAD) {
            flip();
        }
    }

    // -------------------------- core logic --------------------------

    /**
     * Transition from LOAD to JOIN.
     *
     * <p><b>Invocation context</b>: This method runs in a NON-KEYED context. The two callers are
     * (a) {@link #processWatermark2}, which is invoked by the framework without a key context, and
     * (b) {@link #handleIdleFlip}, which fires from the operator-level processing-time service.
     * Therefore {@code flip()} itself must not access keyed state. Per-key work (the buffered probe
     * flush) is delegated to {@link #onEventTime} via {@code timeServiceManager
     * .advanceWatermark(...)} below — that path establishes the correct key context for each fired
     * timer before invoking the callback.
     */
    private void flip() throws Exception {
        if (phase == Phase.JOIN) {
            return;
        }
        phase = Phase.JOIN;
        // Record the flip wall-clock so the TTL handler can grant a grace period of
        // stateTtlMs after the flip before any build-only key becomes eligible for eviction.
        // Without this anchor, keys loaded long before the flip would be evicted as soon as the
        // first TTL fire after the flip happens.
        flipProcTime = getProcessingTimeService().getCurrentProcessingTime();
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
        // Fire all per-key flush timers (TS=1) so any probes buffered during LOAD are joined.
        // We must advance the internal timer service past FLUSH_TIMER_TS even when no probe-side
        // watermark was ever observed (lastProbeWm == Long.MIN_VALUE) — otherwise the buffered
        // probes would sit until a future probe-side WM arrives. Emitting a downstream watermark
        // is gated on a real observed WM so we don't fabricate one.
        // INVARIANT: NS_FLUSH is the ONLY event-time namespace this operator registers. If a new
        // event-time timer is added (e.g. for time-windowed semantics), this advance will fire it
        // prematurely — split the namespaces or scope the advance accordingly.
        long advanceTo = Math.max(lastProbeWm, FLUSH_TIMER_TS);
        if (timeServiceManager != null) {
            timeServiceManager.advanceWatermark(new Watermark(advanceTo));
        }
        if (lastProbeWm != Long.MIN_VALUE) {
            // Sync combinedWatermark[0] with what we emit so the inherited status-path (see
            // processWatermarkStatus override) can never compute and emit a smaller combined WM.
            combinedWatermark.updateWatermark(0, lastProbeWm);
            output.emitWatermark(new Watermark(lastProbeWm));
        }
    }

    private void joinProbeRow(RowData probe) throws Exception {
        boolean matched = false;
        for (Map.Entry<RowData, Long> entry : buildTableState.entries()) {
            RowData buildRow = entry.getKey();
            long count = entry.getValue();
            if (joinCondition.apply(probe, buildRow)) {
                matched = true;
                // Each emitted record uses a fresh JoinedRowData wrapper so test harnesses (or
                // any downstream operator that retains references) see independent rows.
                // Reusing the shared `outRow` here is unsafe when subsequent collects mutate it.
                for (long i = 0; i < count; i++) {
                    JoinedRowData out = new JoinedRowData();
                    out.replace(probe, buildRow);
                    out.setRowKind(RowKind.INSERT);
                    collector.collect(out);
                }
            }
        }
        if (!matched && isLeftOuterJoin) {
            // Same independence guarantee as the matched branch: use a fresh wrapper so a later
            // null-padded emit for a different probe doesn't mutate this record in place.
            JoinedRowData out = new JoinedRowData();
            out.replace(probe, nullPaddedBuild);
            out.setRowKind(RowKind.INSERT);
            collector.collect(out);
        }
    }

    /** Apply a build-side change record directly to the build-table multi-set. */
    private void applyBuildChange(RowData change) throws Exception {
        RowData key = copyWithInsertKind(change);
        Long current = buildTableState.get(key);
        if (RowDataUtil.isAccumulateMsg(change)) {
            buildTableState.put(key, current == null ? 1L : current + 1L);
        } else {
            // -D / -U
            // TODO: a +U following an out-of-order -U for an unseen row is currently treated as
            // a fresh +I and counted as 1. This is a simplification: a stricter implementation
            // would buffer the orphan -U so the matching +U cancels it. Tracked for follow-up.
            if (current == null || current <= 0L) {
                LOG.warn(
                        "Received {} for build row not present in state — ignoring.",
                        change.getRowKind());
                return;
            }
            if (current == 1L) {
                buildTableState.remove(key);
            } else {
                buildTableState.put(key, current - 1L);
            }
        }
        // Arm/refresh the TTL on every write — applies in LOAD and in JOIN.
        refreshTtl();
    }

    private RowData copyWithInsertKind(RowData row) {
        RowData copy = rightSerializer.copy(row);
        copy.setRowKind(RowKind.INSERT);
        return copy;
    }

    private void drainBufferIfPending() throws Exception {
        Long bufferedAt = bufferedAtWmState.value();
        if (bufferedAt == null) {
            return;
        }
        if (phase == Phase.LOAD || latestBuildSideWm > bufferedAt) {
            drainBuffer();
        }
    }

    /**
     * Apply all buffered build-side changes for the current key to {@code buildTableState} in
     * arrival order, then clear the buffer and the {@code bufferedAtWmState}.
     */
    private void drainBuffer() throws Exception {
        List<RowData> changes = new ArrayList<>();
        for (RowData c : buildChangeBuffer.get()) {
            changes.add(rightSerializer.copy(c));
        }
        buildChangeBuffer.clear();
        bufferedAtWmState.clear();
        for (RowData c : changes) {
            applyBuildChange(c);
        }
    }

    private void refreshTtl() throws Exception {
        // TTL is armed regardless of phase so build-only keys (loaded but never matched in JOIN)
        // also expire after stateTtlMs of inactivity. The deadline is reset on every build write
        // and on every probe in JOIN.
        if (stateTtlMs == null) {
            return;
        }
        long deadline = getProcessingTimeService().getCurrentProcessingTime() + stateTtlMs;
        Long previous = ttlExpiryState.value();
        // NOTE: previous and deadline are boxed Longs above the autobox cache range; compare by
        // primitive value, not reference (`==`).
        if (previous != null && previous.longValue() == deadline) {
            return;
        }
        if (previous != null) {
            // Cancel the prior timer to keep the per-key timer-service heap bounded. Without this,
            // every refresh leaves a stale timer alive — under high-rate keys (e.g. probes/sec ×
            // ttl) this is significant state growth even though the firing handler short-circuits
            // stale fires.
            timerService.deleteProcessingTimeTimer(NS_TTL, previous);
        }
        timerService.registerProcessingTimeTimer(NS_TTL, deadline);
        ttlExpiryState.update(deadline);
    }

    // -------------------------- accessors (testing) --------------------------

    @VisibleForTesting
    Phase getPhase() {
        return phase;
    }

    @VisibleForTesting
    long getLatestBuildSideWm() {
        return latestBuildSideWm;
    }

    @VisibleForTesting
    long getLastProbeWm() {
        return lastProbeWm;
    }

    @Nullable
    @VisibleForTesting
    Long getFlipProcTime() {
        return flipProcTime;
    }

    boolean isLeftOuterJoin() {
        return isLeftOuterJoin;
    }

    InternalTypeInfo<RowData> getLeftType() {
        return leftType;
    }

    InternalTypeInfo<RowData> getRightType() {
        return rightType;
    }

    GeneratedJoinCondition getGeneratedJoinCondition() {
        return generatedJoinCondition;
    }

    @Nullable
    Long getLoadCompletedTime() {
        return loadCompletedTime;
    }

    @Nullable
    Long getLoadCompletedIdleTimeoutMs() {
        return loadCompletedIdleTimeoutMs;
    }

    @Nullable
    Long getStateTtlMs() {
        return stateTtlMs;
    }
}
