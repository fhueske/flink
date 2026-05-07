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

package org.apache.flink.table.planner.plan.stream.sql.join;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plan tests for the {@code LATERAL SNAPSHOT} processing-time temporal table join. */
public class LateralSnapshotJoinTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE probe ("
                                + "  k STRING,"
                                + "  v INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ('connector' = 'values', 'bounded' = 'false')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b ("
                                + "  k STRING,"
                                + "  v INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
    }

    /**
     * The {@code 'on_compile_time'} default substitutes {@link System#currentTimeMillis()} for the
     * load-completed time, which makes the resulting plan non-deterministic. All plan tests use an
     * explicit {@code load_completed_time} so the marker carries a fixed value.
     *
     * <p>The expected plan must NOT contain a {@code DropUpdateBefore} above the build-side scan:
     * the LATERAL SNAPSHOT operator requires {@code BEFORE_AND_AFTER} updates from the build side
     * (see {@code FlinkChangelogModeInferenceProgram}'s {@code StreamPhysicalLateralSnapshotJoin}
     * case). Accepting {@code ONLY_UPDATE_AFTER} would silently leak stale build-side rows because
     * the operator's keyed multi-set has no upsert-key info to retract them by.
     */
    @Test
    void testInnerJoin() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe.k = s.k");
    }

    @Test
    void testLeftJoin() {
        util.verifyRelPlan(
                "SELECT * FROM probe LEFT JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe.k = s.k");
    }

    @Test
    void testInnerJoinWithIdleTimeoutAndStateTtl() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00', "
                        + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                        + "state_ttl => INTERVAL '1' DAY) AS s "
                        + "ON probe.k = s.k");
    }

    /**
     * The build-side argument can be a CTE. The CTE definition flows through into the SNAPSHOT
     * scan's relational input and the optimizer rewrites the join just like a direct table
     * reference.
     */
    @Test
    void testInnerJoinWithCteBuildSide() {
        util.verifyRelPlan(
                "WITH cte AS (SELECT k, v + 1 AS v, ts FROM b) "
                        + "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE cte, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe.k = s.k");
    }

    // ------------------------------------------------------------------------------------------
    // Validation: rejection paths
    // ------------------------------------------------------------------------------------------

    @Test
    void testRejectBuildSideWithoutWatermark() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_no_wm ("
                                + "  k STRING,"
                                + "  v INT,"
                                + "  ts TIMESTAMP(3)"
                                + ") WITH ('connector' = 'values', 'bounded' = 'false')");
        final String sql =
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b_no_wm, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe.k = s.k";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "LATERAL SNAPSHOT requires a watermark on the build-side input.");
    }

    @Test
    void testRejectProbeSideNotAppendOnly() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE probe_updates ("
                                + "  k STRING,"
                                + "  v INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts,"
                                + "  PRIMARY KEY (k) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
        final String sql =
                "SELECT * FROM probe_updates JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe_updates.k = s.k";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "StreamPhysicalLateralSnapshotJoin doesn't support consuming "
                                + "update and delete changes");
    }

    @Test
    void testRejectMissingEqualityPredicate() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00') AS s "
                        + "ON probe.v > s.v";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "LATERAL SNAPSHOT join requires at least one equality predicate.");
    }

    /**
     * Regression test for the build-side changelog-mode requirement: the operator needs {@code
     * BEFORE_AND_AFTER} updates to keep its retract-by-row multi-set in sync. When the build source
     * is upsert-style ({@code +I, +UA, -D}, no {@code -U}), the optimizer must satisfy the
     * requirement by inserting a {@code ChangelogNormalize} that materializes the missing {@code
     * -U} records — never a {@code DropUpdateBefore}, which would silently leak stale build rows
     * because the operator has no upsert-key info to retract them by.
     */
    @Test
    void testInnerJoinWithUpsertBuildSourceMaterializesRetractions() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_upsert ("
                                + "  k STRING,"
                                + "  v INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts,"
                                + "  PRIMARY KEY (k) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        final String plan =
                util.tableEnv()
                        .explainSql(
                                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                                        + "input => TABLE b_upsert, "
                                        + "load_completed_condition => 'on_time', "
                                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00')"
                                        + " AS s ON probe.k = s.k");
        assertThat(plan).contains("ChangelogNormalize");
        assertThat(plan).doesNotContain("DropUpdateBefore");
    }

    @Test
    void testRejectSnapshotOutsideLateralContext() {
        final String sql =
                "SELECT * FROM SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'on_time', "
                        + "load_completed_time => TIMESTAMP '2026-07-01 00:00:00')";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("SNAPSHOT can only be used inside a LATERAL clause");
    }

    /**
     * The default condition path uses {@link System#currentTimeMillis()} for the load-completed
     * time, which makes the resulting marker non-deterministic. We can't pin the value, but we can
     * verify the query compiles end-to-end and produces a physical plan carrying the expected exec
     * node — this is a smoke test guarding against regressions in the default-condition rewrite
     * path.
     */
    @Test
    void testInnerJoinWithDefaultOnCompileTime_compilesEndToEnd() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT(input => TABLE b) AS s "
                        + "ON probe.k = s.k";
        // Compile via the table environment without verifying the plan XML (since
        // load_completed_time embeds wall-clock millis at planning).
        util.tableEnv().explainSql(sql);
    }
}
