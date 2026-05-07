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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Harness tests for {@link LateralSnapshotJoinOperator}. */
class LateralSnapshotJoinOperatorTest {

    /** Probe row schema: (id BIGINT, key VARCHAR, val VARCHAR). */
    private static final InternalTypeInfo<RowData> PROBE_TYPE =
            InternalTypeInfo.ofFields(
                    new BigIntType(), VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

    /** Build row schema: (key VARCHAR, val VARCHAR). */
    private static final InternalTypeInfo<RowData> BUILD_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

    /** Probe key column index (key VARCHAR is at field 1). */
    private static final int PROBE_KEY_IDX = 1;

    /** Build key column index (key VARCHAR is at field 0). */
    private static final int BUILD_KEY_IDX = 0;

    private static final RowDataKeySelector PROBE_KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {PROBE_KEY_IDX}, PROBE_TYPE.toRowFieldTypes());
    private static final RowDataKeySelector BUILD_KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {BUILD_KEY_IDX}, BUILD_TYPE.toRowFieldTypes());

    /** Trivial join condition that always matches (equality is enforced by partitioning). */
    private static final String JOIN_FUNC_CODE =
            "public class LateralSnapshotJoinConditionStub extends "
                    + "org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "    public LateralSnapshotJoinConditionStub(Object[] reference) {}\n"
                    + "    @Override public boolean apply("
                    + "        org.apache.flink.table.data.RowData in1,"
                    + "        org.apache.flink.table.data.RowData in2) { return true; }\n"
                    + "}\n";

    private static GeneratedJoinCondition newJoinCondition() {
        return new GeneratedJoinCondition(
                "LateralSnapshotJoinConditionStub", JOIN_FUNC_CODE, new Object[0]);
    }

    private static LateralSnapshotJoinOperator newOperator(
            boolean isLeftOuterJoin,
            Long loadCompletedTime,
            Long loadCompletedIdleTimeoutMs,
            Long stateTtlMs) {
        // Filter nulls on the single equi-key by default (matches what the planner emits for
        // an inner/left equi-join).
        return new LateralSnapshotJoinOperator(
                isLeftOuterJoin,
                PROBE_TYPE,
                BUILD_TYPE,
                newJoinCondition(),
                new boolean[] {true},
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }

    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            newHarness(LateralSnapshotJoinOperator op) throws Exception {
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                op, PROBE_KEY_SELECTOR, BUILD_KEY_SELECTOR, PROBE_KEY_SELECTOR.getProducedType());
    }

    /**
     * Builds a harness using single-field VARCHAR key selectors that correctly handle NULL values
     * (the {@code HandwrittenSelectorUtil} keyselector reads the null bit from the wrong field
     * index when the key is not at column 0).
     */
    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            newHarnessNullSafe(LateralSnapshotJoinOperator op) throws Exception {
        KeySelector<RowData, RowData> probeSel = nullSafeStringKeySelector(PROBE_KEY_IDX);
        KeySelector<RowData, RowData> buildSel = nullSafeStringKeySelector(BUILD_KEY_IDX);
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                op, probeSel, buildSel, PROBE_KEY_SELECTOR.getProducedType());
    }

    private static KeySelector<RowData, RowData> nullSafeStringKeySelector(final int keyIdx) {
        return value -> {
            BinaryRowData ret = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(ret);
            if (value.isNullAt(keyIdx)) {
                writer.setNullAt(0);
            } else {
                writer.writeString(0, value.getString(keyIdx));
            }
            writer.complete();
            return ret;
        };
    }

    // ---------------------------------------------------------------- LOAD phase

    @Test
    void loadPhase_buildSideAppliedDirectly() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // +I, +I (same row dup), -D removes one, leaving 1 occurrence
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement2(deleteRecord("k1", "v1"));
            // -U/+U pair on a different row
            h.processElement2(insertRecord("k1", "v2"));
            h.processElement2(updateBeforeRecord("k1", "v2"));
            h.processElement2(updateAfterRecord("k1", "v2"));

            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            assertThat(stripWatermarks(h.getOutput())).isEmpty();
        }
    }

    @Test
    void loadPhase_probeRecordsBufferedAndWatermarksHeldBack() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();

            h.processElement2(insertRecord("k1", "build1"));
            h.processElement1(insertRecord(1L, "k1", "probe-load-1"));
            h.processElement1(insertRecord(2L, "k1", "probe-load-2"));
            h.processWatermark1(new Watermark(50));
            h.processWatermark2(new Watermark(50));

            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            assertThat(op.getLastProbeWm()).isEqualTo(50L);
            assertThat(op.getLatestBuildSideWm()).isEqualTo(50L);
            // No output (records buffered, watermarks held back).
            assertThat(h.getOutput()).isEmpty();
        }
    }

    // ---------------------------------------------------------------- Flip

    @Test
    void wmTriggeredFlip_drainsProbeBufferAndJoins() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // LOAD: build state and buffered probes
            h.processElement2(insertRecord("k1", "build-k1"));
            h.processElement2(insertRecord("k2", "build-k2"));
            h.processElement1(insertRecord(1L, "k1", "probe-1"));
            h.processElement1(insertRecord(2L, "k2", "probe-2"));
            h.processWatermark1(new Watermark(80));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Flip: build WM crosses loadCompletedTime
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Verify each probe joined against the build row that shares its key (not a
            // cross-product or wrong pairing). Output is INSERT-only.
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(2);
            assertOutputRowsContain(records, 1L, "k1", "probe-1", "k1", "build-k1");
            assertOutputRowsContain(records, 2L, "k2", "probe-2", "k2", "build-k2");
            for (StreamRecord<? extends RowData> rec : records) {
                assertThat(rec.getValue().getRowKind()).isEqualTo(RowKind.INSERT);
            }
            assertThat(h.getOutput()).contains(new Watermark(80L));
        }
    }

    @Test
    void wmTriggeredFlip_leftOuterEmitsNullPaddedWhenNoBuildRow() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(true, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // probe for a key with NO matching build row
            h.processElement1(insertRecord(1L, "kX", "probe-no-match"));
            h.processWatermark1(new Watermark(50));
            h.processWatermark2(new Watermark(100));

            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            RowData out = records.get(0).getValue();
            // probe fields preserved
            assertThat(out.getLong(0)).isEqualTo(1L);
            assertThat(out.getString(1).toString()).isEqualTo("kX");
            // build fields null-padded
            assertThat(out.isNullAt(3)).isTrue();
            assertThat(out.isNullAt(4)).isTrue();
        }
    }

    @Test
    void idleTimeoutTriggersFlip() throws Exception {
        // No loadCompletedTime; only idle timeout
        LateralSnapshotJoinOperator op = newOperator(false, null, 50L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            h.processWatermark1(new Watermark(20));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Advance proc-time past the idle deadline → flip
            h.setProcessingTime(60);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffered probe joined and WM forwarded
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            assertThat(h.getOutput()).contains(new Watermark(20L));
        }
    }

    @Test
    void idleTimerRearmsOnBuildWatermark() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, null, 100L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.setProcessingTime(60);
            // Build WM advances → re-arm
            h.processWatermark2(new Watermark(10));
            // Original idle deadline was 0+100=100. Re-armed at 60+100=160.
            h.setProcessingTime(110);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            h.setProcessingTime(170);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
        }
    }

    @Test
    void flipIsIdempotent_whenWmAndIdleTimerBothCould() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 50L, 100L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            // WM-triggered flip
            h.processWatermark2(new Watermark(60));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            int outputBeforeIdleTimer = h.extractOutputStreamRecords().size();
            assertThat(outputBeforeIdleTimer).isEqualTo(1);
            h.getOutput().clear();
            // Idle timer would have fired at processing-time 100; force the proc-time past that.
            h.setProcessingTime(200);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            // No additional output
            assertThat(h.extractOutputStreamRecords()).hasSize(0);
        }
    }

    // ---------------------------------------------------------------- JOIN phase

    @Test
    void joinPhase_immediateInnerJoin() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Probe in JOIN — joined immediately
            h.processElement1(insertRecord(1L, "k1", "probe-immediate"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            h.getOutput().clear();

            // Probe for non-existent key — no output (INNER)
            h.processElement1(insertRecord(2L, "kX", "probe-no-match"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @Test
    void joinPhase_lazyDrainBuffersBuildSideUntilNextWmAdvance() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffer a -D for v1 (latestBuildSideWm = 100, bufferedAtWm = 100)
            h.processElement2(deleteRecord("k1", "v1"));

            // Probe with same WM — drain check fails (latestBuildSideWm == bufferedAt) → still
            // sees v1 in build state
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            h.getOutput().clear();

            // Advance build-side WM → next access drains the buffer
            h.processWatermark2(new Watermark(200));
            // Now v1 should be deleted; probe sees no match (INNER → no output)
            h.processElement1(insertRecord(2L, "k1", "p2"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @Test
    void joinPhase_buildSideWatermarksNotForwarded() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // triggers flip; emits lastProbeWm if set

            // Build-side WMs in JOIN are suppressed
            h.processWatermark2(new Watermark(200));
            h.processWatermark2(new Watermark(300));

            // Only watermarks from the flip (none here, since lastProbeWm == MIN_VALUE)
            List<Watermark> wms = extractWatermarks(h.getOutput());
            assertThat(wms).isEmpty();

            // Probe-side WMs in JOIN ARE forwarded
            h.processWatermark1(new Watermark(150));
            wms = extractWatermarks(h.getOutput());
            assertThat(wms).containsExactly(new Watermark(150));
        }
    }

    @Test
    void multiKeyIsolation_drainOnKeyA_doesNotAffectKeyB() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("kA", "va"));
            h.processElement2(insertRecord("kB", "vb"));
            h.processWatermark2(new Watermark(100));

            // Buffer a -D for kA only
            h.processElement2(deleteRecord("kA", "va"));
            // Advance build WM
            h.processWatermark2(new Watermark(200));

            // Probe kA → buffer drained, no match (INNER)
            h.processElement1(insertRecord(1L, "kA", "pa"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
            // Probe kB → still has vb, match
            h.processElement1(insertRecord(2L, "kB", "pb"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
        }
    }

    @Test
    void retractOnEmptyState_isIgnored() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Retract a row never inserted — defensively ignored
            h.processElement2(deleteRecord("k1", "ghost"));
            h.processWatermark2(new Watermark(100));
            // Operator must still be functional
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty(); // INNER, no match
        }
    }

    // ---------------------------------------------------------------- TTL

    @Test
    void ttl_clearsBuildStateAfterIdleProcessingTime() throws Exception {
        // stateTtlMs = 50, loadCompletedTime = 100
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip to JOIN
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Probe in JOIN — registers TTL deadline at procTime(0) + 50 = 50
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            h.getOutput().clear();

            // Advance processing time past the TTL deadline → clears build state
            h.setProcessingTime(60);

            // Subsequent probe finds no build row
            h.processElement1(insertRecord(2L, "k1", "p2"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    /**
     * During LOAD, TTL fires must NEVER evict — they're rescheduled past the LOAD phase. Otherwise
     * a long-running LOAD with stateTtlMs shorter than the LOAD duration would drop loaded data
     * before the operator ever serves a probe.
     */
    @Test
    void ttl_doesNotEvictDuringLoad() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 1_000L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            h.processElement2(insertRecord("k1", "v1")); // deadline=50 in LOAD
            // Drive proc-time well past stateTtlMs while still in LOAD.
            h.setProcessingTime(60);
            h.setProcessingTime(150);
            h.setProcessingTime(300);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Trigger flip and probe — k1 must still be present.
            h.processWatermark2(new Watermark(1_000));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
        }
    }

    /**
     * A build-only key (loaded but never matched in JOIN) gets a grace period of {@code stateTtlMs}
     * after the flip before eviction, anchored on the flip processing-time.
     */
    @Test
    void ttl_buildOnlyKeyEvictsAtFlipPlusTtl() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            h.processElement2(insertRecord("k1", "v1")); // initial TTL deadline = 50

            // Trigger flip at proc-time 10. flipProcTime=10, grace ends at 10+50=60.
            h.setProcessingTime(10);
            h.processWatermark2(new Watermark(100));
            assertThat(op.getFlipProcTime()).isEqualTo(10L);

            // At t=50 the original deadline fires; handler sees now=50 < 60 and reschedules
            // the deadline to 60. State is untouched.
            h.setProcessingTime(50);

            // Inside grace window — k1 still present.
            h.setProcessingTime(59);
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
        }
    }

    /**
     * After the grace window closes, a build-only key with no JOIN-phase access must evict. This
     * uses a key that's loaded post-flip so its TTL deadline arms after the grace anchor and is the
     * only deadline in flight.
     */
    @Test
    void ttl_buildOnlyKeyEvictsOutsideGraceWindow() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            h.setProcessingTime(10);
            h.processWatermark2(new Watermark(100)); // flipProcTime=10; grace ends at 60

            // A new build write at t=20 arms a TTL deadline at 20+50=70 (well past the grace
            // window end at 60). When 70 fires, the handler evicts because now=70 >= 60.
            h.setProcessingTime(20);
            h.processElement2(insertRecord("k1", "v1"));
            h.setProcessingTime(70);

            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @Test
    void ttl_armedOnBuildWriteDuringLoad() throws Exception {
        // TTL is armed whenever a build-side change is applied — including during LOAD — so a
        // key that is loaded but never receives a JOIN-phase access still expires (otherwise
        // such "build-only" keys would leak forever).
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark1(new Watermark(10));
            h.processElement1(insertRecord(1L, "k1", "p1")); // buffered probe (no probe TTL)
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // One TTL timer (processing-time) for the build write to k1, and one event-time
            // flush timer for the buffered probe at k1.
            assertThat(h.numEventTimeTimers()).isEqualTo(1);
            assertThat(h.numProcessingTimeTimers()).isEqualTo(1);
        }
    }

    // ---------------------------------------------------------------- Snapshot / restore

    @Test
    void snapshotRestoreInLoad_preservesPhaseAndBufferedRecords() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Trigger flip; the buffered probe should be joined post-restore.
            h.processWatermark1(new Watermark(50));
            h.processWatermark2(new Watermark(100));
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
        }
    }

    /**
     * The {@code flipProcTime} anchor must survive a snapshot/restore cycle so the post-flip grace
     * window is preserved. Otherwise a job restored mid-grace would either over-extend the grace
     * (if reset to {@code now}) or under-extend it (if cleared to {@code null}).
     */
    @Test
    void snapshotRestoreInJoin_preservesFlipProcTime() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, 50L);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(0);
            h.open();
            h.setProcessingTime(7);
            h.processWatermark2(new Watermark(100));
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            assertThat(op1.getFlipProcTime()).isEqualTo(7L);
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            assertThat(op2.getFlipProcTime()).isEqualTo(7L);
        }
    }

    /**
     * Real JOIN→LOAD rescale recovery: combine a JOIN-phase subtask state with a LOAD-phase subtask
     * state via {@link AbstractStreamOperatorTestHarness#repackageState}. After restore, the union
     * list contains both phase entries; the operator must pick LOAD (so a JOIN-rescaled-into-LOAD
     * shape behaves consistently). The recovered keyed state from the JOIN subtask is drained on
     * the next build change.
     */
    @Test
    void rescaleJoinIntoLoad_drainsRecoveredBufferOnNextBuildChange() throws Exception {
        // Subtask A: drive into JOIN with a buffered -D for k1.
        LateralSnapshotJoinOperator opA = newOperator(false, 100L, null, null);
        OperatorSubtaskState stateA;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opA)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(opA.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            // Buffer a -D so it has to be drained on recovery.
            h.processElement2(deleteRecord("k1", "v1"));
            stateA = h.snapshot(0L, 0L);
        }

        // Subtask B: stay in LOAD (no flip-triggering build WM). No keyed state.
        LateralSnapshotJoinOperator opB = newOperator(false, 100L, null, null);
        OperatorSubtaskState stateB;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opB)) {
            h.open();
            assertThat(opB.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            stateB = h.snapshot(0L, 0L);
        }

        // Repackage the two single-subtask states into a single combined state. The union
        // list state contributions from both subtasks become the new operator state.
        OperatorSubtaskState combined =
                AbstractStreamOperatorTestHarness.repackageState(stateA, stateB);

        // Restore the combined state — phase must be LOAD because some subtask was LOAD.
        // The recovered buildTableState and buildChangeBuffer for k1 (from subtask A) are
        // available; the recovery branch in processElement2 must drain them when a new build
        // change arrives.
        LateralSnapshotJoinOperator opC = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opC)) {
            h.initializeState(combined);
            h.open();
            assertThat(opC.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Send a new build change for k1 in the recovered LOAD phase. The recovery branch
            // (processElement2 LOAD-branch when bufferedAtWmState != null) drains the
            // buffered -D first and then applies the new +I.
            h.processElement2(insertRecord("k1", "v2"));
            // Trigger flip via the build-side WM — k1 should now have only v2 (the -D removed
            // v1, the new +I added v2).
            h.processWatermark2(new Watermark(100));
            assertThat(opC.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            h.processElement1(insertRecord(1L, "k1", "p1"));
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            // Probe joined v2 (the new build), not v1 (drained out by the recovered -D).
            assertThat(records.get(0).getValue().getString(4).toString()).isEqualTo("v2");
        }
    }

    @Test
    void snapshotRestoreInJoin_withBufferedBuildChange_drainsOnNextAccess() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            // Buffer a -D in JOIN
            h.processElement2(deleteRecord("k1", "v1"));
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Advance build WM post-restore → next access drains
            h.processWatermark2(new Watermark(200));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            // The -D was applied → no match
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    // ---------------------------------------------------------------- NULL-keys (#1)

    /**
     * Probe rows with a NULL equi-key must not match build rows with a NULL equi-key (SQL
     * semantics: {@code NULL = NULL} is not true).
     */
    @Test
    void nullProbeKey_innerJoin_noMatchEvenIfBuildHasNullKey() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarnessNullSafe(op)) {
            h.open();
            h.processElement2(insertRecord(null, "v_null"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Probe with NULL key — must NOT match build's NULL-keyed row.
            h.processElement1(insertRecord(1L, null, "p_null"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @Test
    void nullProbeKey_leftOuterJoin_emitsNullPaddedEvenIfBuildHasNullKey() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(true, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarnessNullSafe(op)) {
            h.open();
            h.processElement2(insertRecord(null, "v_null"));
            h.processWatermark2(new Watermark(100));

            h.processElement1(insertRecord(1L, null, "p_null"));

            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            RowData out = records.get(0).getValue();
            // Probe fields preserved
            assertThat(out.getLong(0)).isEqualTo(1L);
            assertThat(out.isNullAt(1)).isTrue();
            // Build fields null-padded (not the NULL-keyed build row).
            assertThat(out.isNullAt(3)).isTrue();
            assertThat(out.isNullAt(4)).isTrue();
        }
    }

    // ---------------------------------------------------------------- TTL (#2)

    /**
     * Build keys loaded during LOAD that never receive a probe in JOIN must still expire after
     * stateTtlMs of inactivity. Otherwise such keys leak forever.
     */
    @Test
    void ttl_buildOnlyKeyLoadedDuringLoad_expiresInJoin() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // Load build state for k1 — no probe is ever sent for k1.
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Advance proc-time past TTL deadline (build was written at proc-time 0, so deadline
            // is 50). State should be cleared.
            h.setProcessingTime(60);

            // A probe arriving now must NOT match — the loaded state expired.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    // ---------------------------------------------------------------- Constructor validation (#4)

    @Test
    void constructor_rejectsBothFlipTriggersNull() {
        assertThatThrownBy(() -> newOperator(false, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "loadCompletedTime or loadCompletedIdleTimeoutMs to be configured");
    }

    // ---------------------------------------------------------------- Multi-collect (#17)

    /**
     * When the build multi-set has {@code count > 1} for a key, two emitted records must be
     * independent objects so downstream operators that retain references see distinct rows. A
     * shared {@code outRow} reused across collects would leave both captured records pointing at
     * the (mutated) same instance.
     */
    @Test
    void joinPhase_count_greater_than_one_emitsIndependentRows() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Insert the same build row twice — count = 2.
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement2(insertRecord("k1", "v1"));
            // Two distinct probes for the same key, both buffered.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            h.processElement1(insertRecord(2L, "k1", "p2"));

            // Flip — both probes are flushed sequentially. Each probe produces 2 emits (count=2).
            h.processWatermark2(new Watermark(100));
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(4);

            // Probe1 produced two records each with id=1, probe2 two records with id=2. If
            // outRow were shared and reused, the records captured for probe1 would have been
            // mutated to look like probe2 by the time we inspect them.
            long countProbe1 = records.stream().filter(r -> r.getValue().getLong(0) == 1L).count();
            long countProbe2 = records.stream().filter(r -> r.getValue().getLong(0) == 2L).count();
            assertThat(countProbe1).isEqualTo(2);
            assertThat(countProbe2).isEqualTo(2);
        }
    }

    // ---------------------------------------------------------------- Build-side updates

    /**
     * A {@code -U/+U} pair on the build side replaces the row's value: the multi-set first
     * decrements the old (key, value) entry and then increments the new one. After the flip a probe
     * must match only the new value.
     */
    @Test
    void loadPhase_retractInsertReplacesBuildValue() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Initial value (k1, v1)
            h.processElement2(insertRecord("k1", "v1"));
            // Retract+insert as a value change (k1, v1) -> (k1, v2)
            h.processElement2(updateBeforeRecord("k1", "v1"));
            h.processElement2(updateAfterRecord("k1", "v2"));

            h.processElement1(insertRecord(1L, "k1", "p1"));
            h.processWatermark2(new Watermark(100));

            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            // Only the new build value should match.
            assertThat(records.get(0).getValue().getString(4).toString()).isEqualTo("v2");
        }
    }

    /**
     * In JOIN, a pair of build-side {@code -U}/{@code +U} records buffers atomically: a probe
     * arriving between them must NOT see a half-applied state. Because we drain the buffer only on
     * a build-side WM advance past the buffer's tag, two probes arriving within the same WM window
     * (one before the {@code +U}, one after) both observe the pre-update value.
     */
    @Test
    void joinPhase_probeBetweenRetractAndInsertSeesPreUpdateState() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip to JOIN
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffer the retract half of a -U/+U pair. bufferedAt = current build WM = 100.
            h.processElement2(updateBeforeRecord("k1", "v1"));
            // Probe arrives between the two halves — must see v1 (pre-update).
            h.processElement1(insertRecord(1L, "k1", "between-pair"));
            List<StreamRecord<? extends RowData>> firstProbe = h.extractOutputStreamRecords();
            assertThat(firstProbe).hasSize(1);
            assertThat(firstProbe.get(0).getValue().getString(4).toString()).isEqualTo("v1");
            h.getOutput().clear();

            // Buffer the +U; still no WM advance, so the buffer is not drained.
            h.processElement2(updateAfterRecord("k1", "v2"));
            h.processElement1(insertRecord(2L, "k1", "still-between"));
            List<StreamRecord<? extends RowData>> secondProbe = h.extractOutputStreamRecords();
            assertThat(secondProbe).hasSize(1);
            // Still pre-update — the WM hasn't advanced past `bufferedAt`, drain is gated on
            // the next WM advance (or any access after such advance).
            assertThat(secondProbe.get(0).getValue().getString(4).toString()).isEqualTo("v1");
            h.getOutput().clear();

            // Advance the build-side WM. Next per-key access drains and the +U lands.
            h.processWatermark2(new Watermark(200));
            h.processElement1(insertRecord(3L, "k1", "post-drain"));
            List<StreamRecord<? extends RowData>> postDrain = h.extractOutputStreamRecords();
            assertThat(postDrain).hasSize(1);
            assertThat(postDrain.get(0).getValue().getString(4).toString()).isEqualTo("v2");
        }
    }

    // ---------------------------------------------------------------- WatermarkStatus

    /**
     * Build-side WM idle status changes are absorbed: they must not emit any watermark or watermark
     * status downstream regardless of phase.
     */
    @Test
    void buildSideIdleStatus_isAbsorbed() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // LOAD phase: build-side becomes idle then active again.
            h.processWatermarkStatus2(WatermarkStatus.IDLE);
            h.processWatermarkStatus2(WatermarkStatus.ACTIVE);
            // Drive into JOIN and try the same.
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            h.processWatermarkStatus2(WatermarkStatus.IDLE);
            h.processWatermarkStatus2(WatermarkStatus.ACTIVE);

            // Output may include the flip emit if lastProbeWm was set; here lastProbeWm =
            // MIN_VALUE so no WM is emitted from the flip either. The whole stream must be
            // empty of watermarks and watermark statuses.
            assertThat(extractWatermarks(h.getOutput())).isEmpty();
            assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();
        }
    }

    /**
     * In JOIN, a build-side WM idle status flip must NOT cause the operator to emit a spurious
     * (regression) watermark via the inherited combinedWatermark path. Without our override,
     * AbstractStreamOperator.processWatermarkStatus would emit {@code
     * processWatermark(combinedWatermark.getCombinedWatermark())}, which (with partial[1] never
     * updated) is {@code Long.MIN_VALUE} — far below {@code lastProbeWm}.
     */
    @Test
    void joinPhase_buildSideIdleAfterProbeWm_doesNotRegressWatermark() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip
            // Probe-side WM forwarded downstream.
            h.processWatermark1(new Watermark(150));
            assertThat(extractWatermarks(h.getOutput())).containsExactly(new Watermark(150));
            h.getOutput().clear();

            // Build-side becomes idle: must NOT cause a watermark regression.
            h.processWatermarkStatus2(WatermarkStatus.IDLE);
            assertThat(extractWatermarks(h.getOutput())).isEmpty();
            assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();
        }
    }

    /** Probe-side idle status during LOAD is absorbed (no emits before flip). */
    @Test
    void loadPhase_probeIdleStatus_isAbsorbed() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermarkStatus1(WatermarkStatus.IDLE);
            h.processWatermarkStatus1(WatermarkStatus.ACTIVE);
            assertThat(extractWatermarks(h.getOutput())).isEmpty();
            assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();
        }
    }

    /** Probe-side idle status in JOIN is forwarded downstream. */
    @Test
    void joinPhase_probeIdleStatus_forwarded() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            h.processWatermark1(new Watermark(150));
            h.getOutput().clear();

            h.processWatermarkStatus1(WatermarkStatus.IDLE);
            assertThat(extractWatermarkStatuses(h.getOutput()))
                    .containsExactly(WatermarkStatus.IDLE);
        }
    }

    // ---------------------------------------------------------------- Recovery

    /**
     * A LOAD-phase snapshot that had an idle-flip timer scheduled must arm a fresh idle-flip timer
     * on restore. Otherwise a build-side that goes silent after restore will never trigger the
     * flip.
     */
    @Test
    void snapshotRestoreInLoad_idleFlipTimerRearms() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, null, 100L, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(0);
            h.open();
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, null, 100L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setProcessingTime(50);
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            // Re-armed at open()'s proc-time + idleTimeout = 50 + 100 = 150.
            h.setProcessingTime(149);
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            h.setProcessingTime(150);
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
        }
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Asserts that {@code records} contains a row whose probe and build fields match the given
     * values exactly (id, probeKey, probeVal, buildKey, buildVal).
     */
    private static void assertOutputRowsContain(
            List<StreamRecord<? extends RowData>> records,
            long expectedId,
            String expectedProbeKey,
            String expectedProbeVal,
            String expectedBuildKey,
            String expectedBuildVal) {
        boolean found = false;
        for (StreamRecord<? extends RowData> rec : records) {
            RowData out = rec.getValue();
            if (out.getLong(0) == expectedId
                    && out.getString(1).toString().equals(expectedProbeKey)
                    && out.getString(2).toString().equals(expectedProbeVal)
                    && out.getString(3).toString().equals(expectedBuildKey)
                    && out.getString(4).toString().equals(expectedBuildVal)) {
                found = true;
                break;
            }
        }
        assertThat(found)
                .as(
                        "expected row [id=%d, %s, %s, %s, %s] in %s",
                        expectedId,
                        expectedProbeKey,
                        expectedProbeVal,
                        expectedBuildKey,
                        expectedBuildVal,
                        records)
                .isTrue();
    }

    private static List<Object> stripWatermarks(ConcurrentLinkedQueue<Object> output) {
        List<Object> filtered = new ArrayList<>();
        for (Object o : output) {
            if (!(o instanceof Watermark)) {
                filtered.add(o);
            }
        }
        return filtered;
    }

    private static List<Watermark> extractWatermarks(ConcurrentLinkedQueue<Object> output) {
        List<Watermark> wms = new ArrayList<>();
        for (Object o : output) {
            if (o instanceof Watermark) {
                wms.add((Watermark) o);
            }
        }
        return wms;
    }

    private static List<WatermarkStatus> extractWatermarkStatuses(
            ConcurrentLinkedQueue<Object> output) {
        List<WatermarkStatus> statuses = new ArrayList<>();
        for (Object o : output) {
            if (o instanceof WatermarkStatus) {
                statuses.add((WatermarkStatus) o);
            }
        }
        return statuses;
    }
}
