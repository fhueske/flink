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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

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

        // TODO: add tests (inner + outer) with a different join condition to ensure it is actually
        //  called

        // TODO: add tests (inner + outer) with different filterNullKey behavior to ensure it is
        //  correctly used

        // Filter nulls on the single equi-key by default (matches what the planner emits for
        // an inner/left equi-join).
        // TODO: check if this is true. Is there no way to enforce a different nullKey behavior?
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

            // TODO: do some more?
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
        LateralSnapshotJoinOperator op = newOperator(false, 1000L, 50L, null);
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

    /**
     * Idle-timeout flip with zero build-side elements ever observed: the operator must still
     * advance into JOIN, the buffered probe must be joined against an empty build state, and a
     * subsequent probe must operate normally in JOIN.
     */
    @Test
    void idleTimeoutFlip_withNoBuildElementsEver() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 1000L, 50L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // Buffer a probe; no build elements ever arrive.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);

            // Idle timer fires at proc-time 50 → flip.
            h.setProcessingTime(60);
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffered probe was joined against empty build state — INNER, no output.
            assertThat(h.extractOutputStreamRecords()).isEmpty();

            // A fresh probe in JOIN also returns nothing against the empty build state.
            h.processElement1(insertRecord(2L, "k2", "p2"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @Test
    void idleTimerRearmsOnBuildWatermark() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 1000L, 100L, null);
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

    /**
     * Verifies the {@code processElement2} JOIN-branch drain path: when a new build change arrives
     * after the build-side WM has advanced past the buffer tag, {@code applyBufferedChangesIfReady}
     * runs first to drain the buffer, and THEN the new change is appended (with a fresh {@code
     * bufferedAt} = the now-current WM).
     */
    @Test
    void joinPhase_buildChangeDrainsBufferBeforeAppending() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip to JOIN
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffer a -D at bufferedAt=100. State still has (k1, v1) until the next drain.
            h.processElement2(deleteRecord("k1", "v1"));

            // Advance the build-side WM, but issue the drain via a build change rather than a
            // probe. processElement2 must run applyBufferedChangesIfReady (drains -D), then buffer
            // the new +I (at the new bufferedAt = 200).
            h.processWatermark2(new Watermark(200));
            h.processElement2(insertRecord("k1", "v2"));

            // Advance WM and probe; only the +I should be visible because the -D already drained.
            h.processWatermark2(new Watermark(300));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getValue().getString(4).toString()).isEqualTo("v2");
        }
    }

    /**
     * Tight per-event ordering around a build-side WM advance: a probe before the WM advance sees
     * the pre-buffer state; the WM advance + next probe drains the buffer; the probe after sees the
     * post-drain state.
     */
    @Test
    void joinPhase_eventOrderingAroundBuildWmAdvance() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // (1) Buffer a -D at bufferedAt=100.
            h.processElement2(deleteRecord("k1", "v1"));

            // (2) Probe with no WM advance — sees pre-drain state (v1).
            h.processElement1(insertRecord(1L, "k1", "p1"));
            List<StreamRecord<? extends RowData>> beforeAdvance = h.extractOutputStreamRecords();
            assertThat(beforeAdvance).hasSize(1);
            assertThat(beforeAdvance.get(0).getValue().getString(4).toString()).isEqualTo("v1");
            h.getOutput().clear();

            // (3) Advance build-side WM past bufferedAt.
            h.processWatermark2(new Watermark(200));

            // (4) Probe — drain happens here; the -D applies; no match.
            h.processElement1(insertRecord(2L, "k1", "p2"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    /**
     * When the build multi-set has {@code count > 1} for a key, each matching build row produces a
     * distinct emit (so downstream operators that retain references see independent rows — a
     * shared/reused {@code outRow} would point at the mutated last value), and the {@code
     * matched=true} branch suppresses the LEFT-OUTER null-padded path.
     */
    @ParameterizedTest(name = "leftOuter={0}")
    @CsvSource({"false", "true"})
    void joinPhase_countGreaterThanOne_emitsIndependentMatchesNoNullPad(boolean leftOuter)
            throws Exception {
        LateralSnapshotJoinOperator op = newOperator(leftOuter, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // count(k1, v1) = 2
            h.processElement2(insertRecord("k1", "v1"));
            h.processElement2(insertRecord("k1", "v1"));
            // Two distinct probes for the same key, both buffered.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            h.processElement1(insertRecord(2L, "k1", "p2"));

            // Flip — both probes are flushed; each produces 2 emits (count=2).
            h.processWatermark2(new Watermark(100));
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(4);

            // Independence: probe1 → two id=1 records, probe2 → two id=2 records. A shared/reused
            // outRow would leave probe1's records mutated to look like probe2.
            long countProbe1 = records.stream().filter(r -> r.getValue().getLong(0) == 1L).count();
            long countProbe2 = records.stream().filter(r -> r.getValue().getLong(0) == 2L).count();
            assertThat(countProbe1).isEqualTo(2);
            assertThat(countProbe2).isEqualTo(2);
            // No null-padded row in either INNER or LEFT OUTER: matched=true suppresses the pad.
            for (StreamRecord<? extends RowData> rec : records) {
                RowData out = rec.getValue();
                assertThat(out.isNullAt(3)).isFalse();
                assertThat(out.getString(3).toString()).isEqualTo("k1");
                assertThat(out.getString(4).toString()).isEqualTo("v1");
            }
        }
    }

    // ---------------------------------------------------------------- Build-side updates

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

    /**
     * A second {@code -D} for a row whose count has already dropped to zero must be defensively
     * ignored (logged-and-skipped, not crash or set the count negative). A subsequent {@code +I}
     * for the same row should then restore the count to 1.
     */
    @Test
    void loadPhase_doubleRetractForSingleCountRow_secondIsIgnored() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // count(k1, v1) = 1
            h.processElement2(insertRecord("k1", "v1"));
            // First -D removes the row; count = 0.
            h.processElement2(deleteRecord("k1", "v1"));
            // Second -D for the same (absent) row is ignored.
            h.processElement2(deleteRecord("k1", "v1"));

            // Flip + probe should return no match (state empty for k1).
            h.processWatermark2(new Watermark(100));
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();

            // A fresh +I after the double-retract must put the row back at count=1 (i.e. the
            // ignored second -D did NOT leave any negative bookkeeping).
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(200));
            h.processElement1(insertRecord(2L, "k1", "p2"));
            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getValue().getString(4).toString()).isEqualTo("v1");
        }
    }

    /**
     * The buffered-change drain applies accumulating changes (+I, +U) before retracting changes
     * (-D, -U). For a sequence {@code [-D row, +I row]} buffered while {@code row} is absent from
     * the build state, this reorder yields a final state with the row removed (correct), whereas a
     * naive arrival-order drain would leave the row inserted because the -D would be a no-op.
     */
    @Test
    void joinPhase_bufferedDeleteThenInsertForAbsentRow_endsWithEmptyState() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Drive into JOIN with empty build state for k1.
            h.processElement2(insertRecord("kOther", "vOther"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Buffer [-D, +I] for k1 (absent), all at bufferedAt=100.
            h.processElement2(deleteRecord("k1", "v1"));
            h.processElement2(insertRecord("k1", "v1"));

            // Advance build WM → drain. Reorder: apply +I first (count=1), then -D (count=0,
            // removed). Final state for k1: empty.
            h.processWatermark2(new Watermark(200));

            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

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

    // ---------------------------------------------------------------- NULL-keys

    /**
     * SQL semantics: {@code NULL = NULL} is not true. A probe with a NULL equi-key must never match
     * a NULL-keyed build row, regardless of:
     *
     * <ul>
     *   <li>join type (INNER vs LEFT OUTER) — LEFT OUTER still emits a null-padded row,
     *   <li>arrival phase of the probe — buffered in LOAD then flushed at flip vs. processed
     *       immediately in JOIN. Both exercise the same {@code JoinConditionWithNullFilters}
     *       filter; the first via the NS_FLIP per-key flush, the second via direct join.
     * </ul>
     */
    @ParameterizedTest(name = "leftOuter={0}, probeBufferedInLoad={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void nullProbeKey_neverMatchesNullBuildKey(boolean leftOuter, boolean probeBufferedInLoad)
            throws Exception {
        LateralSnapshotJoinOperator op = newOperator(leftOuter, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarnessNullSafe(op)) {
            h.open();
            h.processElement2(insertRecord(null, "v_null"));
            if (probeBufferedInLoad) {
                h.processElement1(insertRecord(1L, null, "p_null"));
            }
            h.processWatermark2(new Watermark(100));
            if (!probeBufferedInLoad) {
                h.processElement1(insertRecord(1L, null, "p_null"));
            }

            List<StreamRecord<? extends RowData>> records = h.extractOutputStreamRecords();
            if (leftOuter) {
                assertThat(records).hasSize(1);
                RowData out = records.get(0).getValue();
                // Probe fields preserved.
                assertThat(out.getLong(0)).isEqualTo(1L);
                assertThat(out.isNullAt(1)).isTrue();
                // Build fields null-padded — not the NULL-keyed build row's values.
                assertThat(out.isNullAt(3)).isTrue();
                assertThat(out.isNullAt(4)).isTrue();
            } else {
                assertThat(records).isEmpty();
            }
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

    /**
     * Probe-side IDLE status received during LOAD is tracked on partial[0] but not emitted while in
     * LOAD. When the operator transitions to JOIN, the tracked idle state is propagated downstream
     * IFF the probe-side was idle at flip time — i.e. the last-seen status decides.
     *
     * <ul>
     *   <li>IDLE only → emit IDLE at flip.
     *   <li>IDLE → ACTIVE → no emit (last-seen is ACTIVE).
     * </ul>
     */
    @ParameterizedTest(name = "endsActive={0} → expectIdleAtFlip={1}")
    @CsvSource({"false,true", "true,false"})
    void loadPhase_probeIdleStatus_propagatedAtFlipBasedOnLastSeenStatus(
            boolean endsActive, boolean expectIdleEmitted) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermarkStatus1(WatermarkStatus.IDLE);
            if (endsActive) {
                h.processWatermarkStatus1(WatermarkStatus.ACTIVE);
            }
            // Tracked silently — nothing emitted during LOAD.
            assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();

            // Drive into JOIN.
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            if (expectIdleEmitted) {
                assertThat(extractWatermarkStatuses(h.getOutput()))
                        .containsExactly(WatermarkStatus.IDLE);
            } else {
                assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();
            }
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

    // ---------------------------------------------------------------- TTL

    @Test
    void ttl_clearsBuildStateAfterIdleProcessingTime() throws Exception {
        // stateTtlMs = 50, loadCompletedTime = 100. Timers are registered at 1.5 × stateTtlMs,
        // so the actual deadline for an access at t=0 is 75.
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip to JOIN
            assertThat(op.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);

            // Probe in JOIN at t=0 — existing deadline 75 is more than stateTtlMs ahead so the
            // timer is not rearmed.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            h.getOutput().clear();

            // Advance processing time past the TTL deadline (75) → clears build state.
            h.setProcessingTime(80);

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
     * After the grace window closes, a build-only key with no JOIN-phase access must evict. The
     * build state is loaded during LOAD (applied directly to {@code buildTableState}, not buffered)
     * so the TTL eviction has something concrete to clear.
     */
    @Test
    void ttl_buildOnlyKeyEvictsOutsideGraceWindow() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // Load k1 during LOAD — applied directly to buildTableState. TTL deadline at 0+75=75.
            h.processElement2(insertRecord("k1", "v1"));
            // Flip at t=10; flipProcTime=10; grace window ends at 10+50=60.
            h.setProcessingTime(10);
            h.processWatermark2(new Watermark(100));
            assertThat(op.getFlipProcTime()).isEqualTo(10L);

            // At t=75 the TTL timer fires. Grace check: now=75 < flipProcTime+stateTtlMs=60 is
            // false → eviction. buildTableState for k1 is cleared.
            h.setProcessingTime(75);

            // Probe arriving after eviction finds no build row.
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

    /**
     * Boundary behavior of the per-access TTL rearm. With stateTtlMs=50 and the 1.5× registration
     * multiplier, a build write at t=0 arms a timer at t=75:
     *
     * <ul>
     *   <li>Access at t=10 leaves the timer 65ms away (≥ stateTtlMs) → rearm skipped. At t=80 the
     *       original 75-deadline has fired and evicted the row.
     *   <li>Access at t=30 leaves the timer 45ms away (&lt; stateTtlMs) → rearmed to t=30+75=105.
     *       At t=80 the row is still alive because the deadline moved past it.
     * </ul>
     */
    @ParameterizedTest(name = "accessTime={0} → expectRowAtT80={1}")
    @CsvSource({"10,false", "30,true"})
    void ttl_perAccessRearmBoundary(long accessTime, boolean expectRowAtT80) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // Build write at t=0 arms timer at 0 + 75 = 75.
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100)); // flip to JOIN

            // Per-access touch at the parameterized time decides skip vs. rearm.
            h.setProcessingTime(accessTime);
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
            h.getOutput().clear();

            // At t=80: if the rearm happened, the deadline moved past 80 and the row is still
            // present; if the rearm was skipped, the 75-deadline fired and evicted.
            h.setProcessingTime(80);
            h.processElement1(insertRecord(2L, "k1", "p2"));
            assertThat(h.extractOutputStreamRecords()).hasSize(expectRowAtT80 ? 1 : 0);
        }
    }

    /**
     * A snapshot/restore cycle re-anchors {@code flipProcTime} on the restore-time clock. The
     * post-flip TTL grace window therefore extends from the restore point, not the original flip.
     * This is a deliberate trade-off: a job that restarts within {@code stateTtlMs} keeps
     * build-only keys alive even though they would otherwise have been eligible for eviction.
     */
    @Test
    void ttl_restoreReanchorsFlipProcTime_extendingGraceWindow() throws Exception {
        // stateTtlMs = 50. Build write at t=0 arms TTL at 75. Flip at t=0 → flipProcTime=0.
        // Original grace ends at 0+50=50.
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, 50L);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(0);
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op1.getFlipProcTime()).isEqualTo(0L);
            state = h.snapshot(0L, 0L);
        }

        // Restart at t=30 — flipProcTime is re-anchored to 30; new grace ends at 30+50=80.
        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setProcessingTime(30);
            h.initializeState(state);
            h.open();
            assertThat(op2.getFlipProcTime()).isEqualTo(30L);

            // At t=75 the recovered TTL timer fires. Grace check: now=75 < flipProcTime+stateTtlMs
            // = 80 → reschedule rather than evict.
            h.setProcessingTime(75);

            // k1 still present because the grace window was re-anchored.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).hasSize(1);
        }
    }

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

            // Advance proc-time past the TTL deadline. The build was written at proc-time 0 and
            // timers register at 1.5 × stateTtlMs, so the deadline is 75. State should be cleared
            // after t=75.
            h.setProcessingTime(80);

            // A probe arriving now must NOT match — the loaded state expired.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
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
     * On restore into JOIN, {@code flipProcTime} is anchored on the current processing-time clock
     * rather than the original flip time. {@code flipProcTime} is not persisted; this re-arms the
     * TTL post-flip grace window from the restore point on every restore.
     */
    @Test
    void snapshotRestoreInJoin_anchorsFlipProcTimeOnRestoreTime() throws Exception {
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
            h.setProcessingTime(9L);
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            assertThat(op2.getFlipProcTime()).isEqualTo(9L);
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

    /**
     * Recovery into JOIN with a buffered build change and NO subsequent build-side watermark: the
     * buffer would otherwise sit forever because the drain gate {@code latestBuildSideWm >
     * bufferedAt} can never be satisfied (latestBuildSideWm is reset to {@code MIN_VALUE} on
     * restore). The operator detects this case via {@code latestBuildSideWm == MIN_VALUE} and
     * drains eagerly on the first per-key access.
     */
    @Test
    void snapshotRestoreInJoin_withBufferedBuildChange_noFurtherWm_drainsEagerly()
            throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.open();
            h.processElement2(insertRecord("k1", "v1"));
            h.processWatermark2(new Watermark(100));
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            // Buffer a -D at bufferedAt=100.
            h.processElement2(deleteRecord("k1", "v1"));
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertThat(op2.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.JOIN);
            assertThat(op2.getLatestBuildSideWm()).isEqualTo(Long.MIN_VALUE);

            // No build WM arrives. The probe must still see the post-drain state, otherwise the
            // recovered -D would never apply.
            h.processElement1(insertRecord(1L, "k1", "p1"));
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    /**
     * A LOAD-phase snapshot that had an idle-flip timer scheduled must arm a fresh idle-flip timer
     * on restore. Otherwise a build-side that goes silent after restore will never trigger the
     * flip.
     */
    @Test
    void snapshotRestoreInLoad_idleFlipTimerRearms() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 1000L, 100L, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(0);
            h.open();
            assertThat(op1.getPhase()).isEqualTo(LateralSnapshotJoinOperator.Phase.LOAD);
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 1000L, 100L, null);
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
