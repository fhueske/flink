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
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
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
    private static final String BUILD_TABLE_STATE_NAME = "build-table";
    private static final String BUILD_CHANGE_BUFFER_STATE_NAME = "build-change-buffer";
    private static final String BUFFERED_AT_WM_STATE_NAME = "buffered-at-wm";
    private static final String PROBE_BUFFER_STATE_NAME = "probe-buffer";
    private static final String TTL_EXPIRY_STATE_NAME = "ttl-expiry";

    private static final String TIMER_SERVICE_NAME = "lateral-snapshot-timers";

    @VisibleForTesting static final String NS_FLIP = "flip";
    @VisibleForTesting static final String NS_TTL = "ttl";

    /**
     * Event-time timestamp at which the per-key {@code probeBuffer} flip join timer is registered.
     * Any non-{@code MIN_VALUE} watermark advance fires it.
     */
    @VisibleForTesting static final long FLIP_JOIN_TIMER_TS = 1L;

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
     * Timestamp at which the build-side watermark must arrive for the operator to flip from {@code
     * LOAD} to {@code JOIN}.
     */
    private final Long loadCompletedTime;

    /**
     * Processing-time idle timeout duration (millis) on build-side watermarks. When configured, the
     * operator flips to {@code JOIN} if no build-side watermark advance is seen for this duration.
     */
    @Nullable private final Long loadCompletedIdleTimeoutMs;

    /**
     * State TTL (millis) to clean up any keyed state during JOIN phase. We schedule TTL timers
     * maxStateTtlMs ahead and check on minStateTtlMs before scheduling a new timer. This avoids
     * rescheduling timers on every state access while still ensuring that keyed state is evicted
     * after at most maxStateTtlMs of key inactivity during JOIN phase. If minStateTtlMs is set to
     * 0, state TTL is disabled.
     */
    private final long minStateTtlMs;

    private final long maxStateTtlMs;

    // -------------------------- transient runtime --------------------------

    private transient JoinConditionWithNullFilters joinCondition;
    private transient GenericRowData nullPaddedBuild;
    private transient TimestampedCollector<RowData> collector;

    private transient InternalTimerService<String> timerService;

    private transient Phase phase;

    /**
     * Processing-time wall clock at which the operator transitioned from {@link Phase#LOAD} to
     * {@link Phase#JOIN}. {@code null} while still in {@code LOAD}. Used by the TTL handler to
     * reschedule state TTL timers that fire too early.
     */
    @Nullable private transient Long flipProcTime;

    /** Highest build-side watermark observed; not persisted. */
    private transient long latestBuildSideWm;

    /** Latest probe-side watermark observed during LOAD; forwarded on flip. */
    private transient long lastProbeWm;

    /** Non-keyed processing-time idle-flip timer. */
    @Nullable private transient ScheduledFuture<?> idleFlipTimer;

    // -------------------------- keyed state --------------------------

    /** Build-side table as multi-set: row → reference count. */
    private transient MapState<RowData, Long> buildTableState;

    /** Buffer for build-side changes during JOIN to ensure atomic updates. */
    private transient ListState<RowData> buildChangeBuffer;

    /** Build-side watermark to ensure atomic application of build changes during JOIN. */
    private transient ValueState<Long> bufferedAtWmState;

    /** Buffer for probe-side records during LOAD. */
    private transient ListState<RowData> probeBuffer;

    /** Most recently registered TTL timer deadline; used to advance TTL timer. */
    private transient ValueState<Long> ttlExpiryState;

    // -------------------------- operator state --------------------------

    private transient ListState<String> operatorPhaseState;

    public LateralSnapshotJoinOperator(
            boolean isLeftOuterJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            boolean[] filterNullKeys,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.leftType = Preconditions.checkNotNull(leftType);
        this.rightType = Preconditions.checkNotNull(rightType);
        this.generatedJoinCondition = Preconditions.checkNotNull(generatedJoinCondition);
        this.filterNullKeys = Preconditions.checkNotNull(filterNullKeys);
        this.loadCompletedTime = Preconditions.checkNotNull(loadCompletedTime);
        if (this.loadCompletedTime < 0) {
            throw new IllegalArgumentException("loadCompletedTime must be non-negative");
        }
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        if (this.loadCompletedIdleTimeoutMs != null && this.loadCompletedIdleTimeoutMs < 0) {
            throw new IllegalArgumentException("loadCompletedIdleTimeoutMs must be non-negative");
        }
        this.minStateTtlMs = stateTtlMs == null ? 0 : stateTtlMs;
        if (this.minStateTtlMs < 0) {
            throw new IllegalArgumentException("stateTtlMs must be non-negative");
        }
        // maxStateTtlMs is 1.5x of minStateTtlMs
        this.maxStateTtlMs = this.minStateTtlMs + this.minStateTtlMs / 2;
    }

    // -------------------------- lifecycle --------------------------

    @Override
    public boolean useInterruptibleTimers(ReadableConfig config) {
        return true;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Operator state only — keyed state and timer services are initialized in open()
        operatorPhaseState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        OPERATOR_PHASE_STATE_NAME, StringSerializer.INSTANCE));

        // any LOAD entry → LOAD; empty (fresh start) → LOAD; else JOIN
        boolean phaseStateExists = false;
        boolean anyTaskInLoad = false;
        for (String phase : operatorPhaseState.get()) {
            phaseStateExists = true;
            if (Phase.LOAD.name().equals(phase)) {
                anyTaskInLoad = true;
                break;
            }
        }
        // we are in LOAD phase if no phaseState exists (no savepoint/checkpoint) or any task was
        // still in LOAD phase (not all tasks transitioned to JOIN phaase).
        phase = (!phaseStateExists || anyTaskInLoad) ? Phase.LOAD : Phase.JOIN;

        // When restored into JOIN, anchor flipProcTime on the current wall clock so the TTL
        // handler's post-flip grace window restarts from now.
        flipProcTime =
                phase == Phase.JOIN ? getProcessingTimeService().getCurrentProcessingTime() : null;

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
        // equi-keys whose values may be NULL.
        final JoinCondition rawCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        joinCondition = new JoinConditionWithNullFilters(rawCondition, filterNullKeys, this);
        joinCondition.setRuntimeContext(getRuntimeContext());
        joinCondition.open(DefaultOpenContext.INSTANCE);

        nullPaddedBuild = new GenericRowData(rightType.toRowType().getFieldCount());
        collector = new TimestampedCollector<>(output);

        timerService = getInternalTimerService(TIMER_SERVICE_NAME, StringSerializer.INSTANCE, this);

        // Mark the build-side input (index 1) as permanently idle in the inherited
        // combinedWatermark accounting. This operator never forwards build-side WMs nor
        // build-side idle status: it absorbs both.
        combinedWatermark.updateStatus(1, true);

        // Register the load-completed idle-timeout timer if it is configured.
        if (phase == Phase.LOAD && loadCompletedIdleTimeoutMs != null) {
            scheduleIdleFlipTimer();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        operatorPhaseState.update(List.of(phase.name()));
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
            timerService.registerEventTimeTimer(NS_FLIP, FLIP_JOIN_TIMER_TS);
        } else {
            applyBufferedChangesIfReady();
            joinProbeRow(probe);
        }
        refreshStateTtl();
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData build = element.getValue();
        if (phase == Phase.LOAD) {
            Long bufferedAt = bufferedAtWmState.value();
            if (bufferedAt != null) {
                // Recovery from a restart. The key was in JOIN phase before.
                // We apply all buffered changes before applying the current change.
                applyBufferedChanges();
            }
            // during LOAD apply change directly
            applyBuildChange(build);
        } else {
            // apply buffered changes if build-side WM was advanced.
            applyBufferedChangesIfReady();
            // during JOIN, buffer the change and only apply after build-side WM advanced
            buildChangeBuffer.add(build);
            bufferedAtWmState.update(latestBuildSideWm);
        }
        refreshStateTtl();
    }

    // -------------------------- watermarks --------------------------

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        // Probe-side watermark.
        if (phase == Phase.LOAD) {
            // in LOAD, just keep track of the latest probe-side wm.
            lastProbeWm = Math.max(lastProbeWm, mark.getTimestamp());
            // do not advance timer service, do not forward
        } else {
            // in JOIN, forward the probe-side wm downstream.
            super.processWatermark1(mark);
        }
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        // Build-side watermark: NEVER forwarded; never advances the timer service.
        long ts = mark.getTimestamp();
        latestBuildSideWm = Math.max(latestBuildSideWm, ts);
        if (phase == Phase.LOAD) {
            if (latestBuildSideWm >= loadCompletedTime) {
                // we reached the flip point. Transition to JOIN phase.
                transitionToJoinPhase();
            } else if (loadCompletedIdleTimeoutMs != null) {
                // we got a new build-side wm. Reschedule the idle timer (if it was configured)
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
            // During LOAD, nothing is emitted downstream — neither watermarks nor status. But we
            // do track partial[0]'s idle bit so that, after the flip, the operator has an accurate
            // view of the probe-side's idle state.
            combinedWatermark.updateStatus(0, watermarkStatus.isIdle());
            return;
        }
        super.processWatermarkStatus(watermarkStatus, index);
    }

    // -------------------------- timers --------------------------

    @Override
    public void onEventTime(InternalTimer<RowData, String> timer) throws Exception {
        String ns = timer.getNamespace();
        // the NS_FLIP timers are fired when the operator transitions from LOAD to JOIN phase.
        if (NS_FLIP.equals(ns)) {
            // In case, a recovery happened before, there might be buffered build-side changes.
            // Apply them before joining the buffered probe-side records.
            applyBufferedChanges();
            // Join each buffered probe row.
            for (RowData p : probeBuffer.get()) {
                joinProbeRow(p);
            }
            probeBuffer.clear();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, String> timer) throws Exception {
        // TTL timers run on processing time so semantics match Flink's standard StateTtlConfig.
        if (!NS_TTL.equals(timer.getNamespace())) {
            return;
        }
        if (minStateTtlMs == 0) {
            // TTL wasn't configured and shouldn't have registered any timers.
            return;
        }
        Long deadline = ttlExpiryState.value();
        if (deadline == null || timer.getTimestamp() != deadline) {
            return; // stale timer fire
        }
        long now = getProcessingTimeService().getCurrentProcessingTime();
        // check if we need to reschedule the ttl timer. This is necessary if
        //   a) we're still in LOAD phase, or
        //   b) if we're in JOIN but the flip happened less than stateTtlMs ago.
        if (phase == Phase.LOAD || (flipProcTime != null && now < flipProcTime + minStateTtlMs)) {
            // set the new TTL timer maxStateTtlMs ahead
            long newDeadline =
                    phase == Phase.LOAD ? now + maxStateTtlMs : flipProcTime + maxStateTtlMs;
            timerService.registerProcessingTimeTimer(NS_TTL, newDeadline);
            ttlExpiryState.update(newDeadline);
            return;
        }
        // clear all per-key state
        buildTableState.clear();
        buildChangeBuffer.clear();
        bufferedAtWmState.clear();
        ttlExpiryState.clear();
        // probeBuffer should be empty because we are in JOIN phase, but clear out just in case.
        probeBuffer.clear();
    }

    /**
     * Registers the load-completion idle-timeout timer. No-op when the timeout is not configured.
     */
    private void scheduleIdleFlipTimer() {
        if (loadCompletedIdleTimeoutMs == null) {
            return;
        }
        long deadline =
                getProcessingTimeService().getCurrentProcessingTime() + loadCompletedIdleTimeoutMs;
        idleFlipTimer =
                getProcessingTimeService().registerTimer(deadline, t -> transitionToJoinPhase());
    }

    /** Updates the idle flip timer. */
    private void rescheduleIdleFlipTimer() {
        cancelIdleFlipTimer();
        scheduleIdleFlipTimer();
    }

    /** Deactivates the currently registered idle flip timer. */
    private void cancelIdleFlipTimer() {
        if (idleFlipTimer != null) {
            idleFlipTimer.cancel(false);
            idleFlipTimer = null;
        }
    }

    // -------------------------- core logic --------------------------

    /**
     * Transition from LOAD to JOIN.
     *
     * <p><b>Invocation context</b>: This method runs in a NON-KEYED context. The two callers are
     * (a) {@link #processWatermark2}, which is invoked by the framework without a key context, and
     * (b) {@link #idleFlipTimer}, which fires from the operator-level processing-time service.
     * Therefore {@code flipToJoinPhase()} itself must not access keyed state. Per-key work (the
     * buffered probe flush) is delegated to {@link #onEventTime} via {@code timeServiceManager
     * .advanceWatermark(...)} below — that path establishes the correct key context for each fired
     * timer before invoking the callback.
     */
    private void transitionToJoinPhase() throws Exception {
        if (phase == Phase.JOIN) {
            return;
        }
        phase = Phase.JOIN;
        // Record the flip wall-clock so the TTL handler can grant a grace period of
        // stateTtlMs after the flip before any build-only key becomes eligible for eviction.
        // Without this anchor, keys loaded long before the flip would be evicted as soon as the
        // first TTL fire after the flip happens.
        flipProcTime = getProcessingTimeService().getCurrentProcessingTime();
        // disable idle flip timer
        cancelIdleFlipTimer();
        // Fire all per-key flip timers (TS=1) so any probes buffered during LOAD are joined.
        long advanceTo = Math.max(lastProbeWm, FLIP_JOIN_TIMER_TS);
        if (timeServiceManager != null) {
            timeServiceManager.advanceWatermark(new Watermark(advanceTo));
        }
        // Emit the last observed probe-side wm downstream
        if (lastProbeWm != Long.MIN_VALUE) {
            combinedWatermark.updateWatermark(0, lastProbeWm);
            output.emitWatermark(new Watermark(lastProbeWm));
        }
        // If the probe-side was idle at flip time (tracked during LOAD via
        // processWatermarkStatus), propagate the idle status downstream now so watermark
        // alignment in downstream operators stays consistent.
        if (combinedWatermark.isIdle()) {
            output.emitWatermarkStatus(WatermarkStatus.IDLE);
        }
    }

    /**
     * Joins a probe-side row against the current build-side table and applies the join predicate.
     * Returns a null-padded result if the row doesn't match any build-side row and this is a LEFT
     * OUTER join.
     */
    private void joinProbeRow(RowData probe) throws Exception {
        boolean matched = false;
        for (Map.Entry<RowData, Long> entry : buildTableState.entries()) {
            RowData buildRow = entry.getKey();
            long count = entry.getValue();
            if (joinCondition.apply(probe, buildRow)) {
                matched = true;
                // Each emitted record uses a fresh JoinedRowData wrapper.
                // Reusing a row object here is unsafe when subsequent collects mutate it.
                for (long i = 0; i < count; i++) {
                    JoinedRowData out = new JoinedRowData();
                    out.replace(probe, buildRow);
                    out.setRowKind(RowKind.INSERT);
                    collector.collect(out);
                }
            }
        }
        if (!matched && isLeftOuterJoin) {
            // No join match, emit a null-padded LEFT OUTER join result
            JoinedRowData out = new JoinedRowData();
            out.replace(probe, nullPaddedBuild);
            out.setRowKind(RowKind.INSERT);
            collector.collect(out);
        }
    }

    /**
     * Applies the buffered build-side changes if the build-side watermark advanced since last
     * buffer application. This ensures that we apply buffered changes atomically once their
     * corresponding build-side WM is passed.
     */
    private void applyBufferedChangesIfReady() throws Exception {
        Long bufferedAt = bufferedAtWmState.value();
        if (bufferedAt != null && latestBuildSideWm > bufferedAt) {
            // the build-side wm advanced. Buffered changes can be applied atomically now.
            applyBufferedChanges();
        } else if (latestBuildSideWm == Long.MIN_VALUE) {
            // No build-side watermark has ever been observed by this subtask. This happens
            //   (a) after a recovery (e.g. JOIN-phase rescaled into this subtask), or
            //   (b) when the operator flipped via the idle-timeout fallback and no build-side
            //       watermark arrived afterwards.
            // In either case we want to apply the buffered changes now because we do not know
            // when the next build-side WM will arrive (if it ever will). This ensures that we don't
            // indefinitely defer buffered.
            applyBufferedChanges();
        }
    }

    /**
     * Apply all buffered build-side changes for the current key to {@code buildTableState}.
     * Accumulating changes (+I, +U) are applied before retracting changes are applied (-D, -U).
     * Afterward, the buffer state is cleared.
     */
    private void applyBufferedChanges() throws Exception {
        List<RowData> retractions = new ArrayList<>();
        // Apply accumulating changes (+I, +U) first, deferring retractions to a second pass.
        for (RowData c : buildChangeBuffer.get()) {
            if (RowDataUtil.isAccumulateMsg(c)) {
                applyBuildChange(c);
            } else {
                retractions.add(c);
            }
        }
        // Then apply the deferred retractions (-D, -U).
        for (RowData c : retractions) {
            applyBuildChange(c);
        }
        buildChangeBuffer.clear();
        bufferedAtWmState.clear();
    }

    /**
     * Apply a build-side change record directly to the build-table multi-set.
     *
     * <p>MUTATES the input row's {@link RowKind} to {@link RowKind#INSERT} to normalize the key
     * used for {@code buildTableState} lookups. The caller must not rely on the original kind after
     * this call returns. The mutation avoids a per-record copy on a hot path.
     */
    private void applyBuildChange(RowData change) throws Exception {
        RowKind changeType = change.getRowKind();
        change.setRowKind(RowKind.INSERT);
        Long currentCnt = buildTableState.get(change);
        if (changeType == RowKind.INSERT || changeType == RowKind.UPDATE_AFTER) {
            // +I / +U
            buildTableState.put(change, currentCnt == null ? 1L : currentCnt + 1L);
        } else {
            // -D / -U
            if (currentCnt == null || currentCnt <= 0L) {
                // TODO: check if there is a metric to report unsuccessful retractions
                LOG.warn("Received {} for build row not present in state — ignoring.", changeType);
                return;
            }
            if (currentCnt == 1L) {
                buildTableState.remove(change);
            } else {
                buildTableState.put(change, currentCnt - 1L);
            }
        }
    }

    /** If state TTL is configured, refreshes the state TTL timer if needed. */
    private void refreshStateTtl() throws Exception {
        if (minStateTtlMs == 0) {
            // Nothing to do when state TTL is not configured.
            return;
        }
        // We register it at maxStateTtlMs to avoid rearming the timer on every access.
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long refreshThreshold = now + minStateTtlMs;
        Long currentTtlTimer = ttlExpiryState.value();
        if (currentTtlTimer != null && currentTtlTimer >= refreshThreshold) {
            // Existing timer still covers at least one full stateTtlMs — leave it in place.
            return;
        }
        if (currentTtlTimer != null) {
            // Remove the current timer before setting a new one.
            timerService.deleteProcessingTimeTimer(NS_TTL, currentTtlTimer);
        }
        long newDeadline = now + maxStateTtlMs;
        timerService.registerProcessingTimeTimer(NS_TTL, newDeadline);
        ttlExpiryState.update(newDeadline);
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
}
