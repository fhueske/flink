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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end SQL IT tests for {@code LATERAL SNAPSHOT(TABLE ...)}. */
class LateralSnapshotJoinITCase extends StreamingTestBase {

    /** Build-side rows: (product_id, name, ts). */
    private static final List<Row> PRODUCTS =
            Arrays.asList(
                    Row.of(1, "A", LocalDateTime.parse("1970-01-01T00:00:01")),
                    Row.of(2, "B", LocalDateTime.parse("1970-01-01T00:00:02")),
                    Row.of(3, "C", LocalDateTime.parse("1970-01-01T00:00:03")));

    /** Probe-side rows: (order_id, product_id, ts). */
    private static final List<Row> ORDERS =
            Arrays.asList(
                    Row.of(101, 1, LocalDateTime.parse("1970-01-01T00:00:10")),
                    Row.of(102, 2, LocalDateTime.parse("1970-01-01T00:00:11")),
                    Row.of(103, 3, LocalDateTime.parse("1970-01-01T00:00:12")),
                    // probe row whose key has no match
                    Row.of(104, 99, LocalDateTime.parse("1970-01-01T00:00:13")));

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv().getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);

        String productsDataId = TestValuesTableFactory.registerData(PRODUCTS);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE products (\n"
                                        + "  id INT,\n"
                                        + "  name STRING,\n"
                                        + "  ts TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR ts AS ts\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'bounded' = 'false',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                productsDataId));

        String ordersDataId = TestValuesTableFactory.registerData(ORDERS);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE orders (\n"
                                        + "  id INT,\n"
                                        + "  product_id INT,\n"
                                        + "  ts TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR ts AS ts\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'bounded' = 'false',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                ordersDataId));
    }

    @Test
    void innerJoinWithExplicitLoadCompletedTime() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink (\n"
                                + "  order_id INT,\n"
                                + "  product_name STRING\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO sink\n"
                                + "SELECT o.id, p.name\n"
                                + "FROM orders o JOIN LATERAL SNAPSHOT(\n"
                                + "  input => TABLE products,\n"
                                + "  load_completed_condition => 'on_time',\n"
                                + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                + ") AS p\n"
                                + "ON o.product_id = p.id")
                .await();

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder("+I[101, A]", "+I[102, B]", "+I[103, C]");
    }

    /**
     * Probe rows whose event-time timestamp falls before the {@code load_completed_time} arrive
     * during the LOAD phase and must be buffered until the flip. A naive operator that joined
     * probes against build state as soon as both arrive (without waiting for the LOAD phase to
     * complete) would produce wrong results — for instance, an early probe arriving before any
     * matching build row would emit nothing for an INNER JOIN. This test pins the build-side
     * watermark behind the load_completed_time so probes are forced to wait.
     */
    @Test
    void innerJoinBuffersProbesArrivingDuringLoadPhase() throws Exception {
        // Override probe data so probe timestamps are EARLIER than load_completed_time.
        tEnv().getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);
        String earlyOrdersDataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                // probe ts = 00:00:01, before flip at 00:00:05
                                Row.of(201, 1, LocalDateTime.parse("1970-01-01T00:00:01")),
                                Row.of(202, 2, LocalDateTime.parse("1970-01-01T00:00:01"))));
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE early_orders (\n"
                                        + "  id INT,\n"
                                        + "  product_id INT,\n"
                                        + "  ts TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR ts AS ts\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'bounded' = 'false',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                earlyOrdersDataId));

        tEnv().executeSql(
                        "CREATE TABLE sink_buffered (\n"
                                + "  order_id INT,\n"
                                + "  product_name STRING\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO sink_buffered\n"
                                + "SELECT o.id, p.name\n"
                                + "FROM early_orders o JOIN LATERAL SNAPSHOT(\n"
                                + "  input => TABLE products,\n"
                                + "  load_completed_condition => 'on_time',\n"
                                + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                + ") AS p\n"
                                + "ON o.product_id = p.id")
                .await();

        // Both probes arrived before the build-side watermark crossed load_completed_time, so
        // they were buffered and joined post-flip. Without buffering they would produce no
        // output.
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_buffered"))
                .containsExactlyInAnyOrder("+I[201, A]", "+I[202, B]");
    }

    /**
     * The default {@code load_completed_condition = 'on_compile_time'} substitutes the planner's
     * wall-clock time at compile. Build-side timestamps in this test are far in the past
     * (1970-01-01), so the build-side WM crosses the compile-time gate immediately on the first
     * source-emitted MAX_WATERMARK, and the operator flips. End-to-end IT coverage of this path
     * (the FLIP's default) was previously only smoke-tested at the plan level.
     */
    @Test
    void innerJoinWithDefaultOnCompileTime() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink (\n"
                                + "  order_id INT,\n"
                                + "  product_name STRING\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO sink\n"
                                + "SELECT o.id, p.name\n"
                                + "FROM orders o JOIN LATERAL SNAPSHOT(input => TABLE products) AS p\n"
                                + "ON o.product_id = p.id")
                .await();

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder("+I[101, A]", "+I[102, B]", "+I[103, C]");
    }

    /**
     * End-to-end coverage of build-side updates: a {@code -U/+U} pair on the build side that
     * arrives entirely in the LOAD phase must replace the row's value, and post-flip probes must
     * see only the new value. Without bug #1's fix (visitor preferring {@code BEFORE_AND_AFTER}), a
     * {@code DropUpdateBefore} would silently strip the {@code -U} and the operator would leak the
     * original value.
     */
    @Test
    void innerJoinWithBuildSideUpdates() throws Exception {
        // Build a CDC-style products source with full retraction (I,UB,UA,D) so updates retract.
        // (k=1, name="A") -> (k=1, name="A2") via -U/+U pair, all in LOAD phase.
        String productsCdcId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(
                                        org.apache.flink.types.RowKind.INSERT,
                                        1,
                                        "A",
                                        LocalDateTime.parse("1970-01-01T00:00:01")),
                                Row.ofKind(
                                        org.apache.flink.types.RowKind.UPDATE_BEFORE,
                                        1,
                                        "A",
                                        LocalDateTime.parse("1970-01-01T00:00:02")),
                                Row.ofKind(
                                        org.apache.flink.types.RowKind.UPDATE_AFTER,
                                        1,
                                        "A2",
                                        LocalDateTime.parse("1970-01-01T00:00:02")),
                                Row.ofKind(
                                        org.apache.flink.types.RowKind.INSERT,
                                        2,
                                        "B",
                                        LocalDateTime.parse("1970-01-01T00:00:03"))));
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE products_cdc (\n"
                                        + "  id INT,\n"
                                        + "  name STRING,\n"
                                        + "  ts TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR ts AS ts\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'bounded' = 'false',\n"
                                        + "  'changelog-mode' = 'I,UB,UA,D',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                productsCdcId));

        tEnv().executeSql(
                        "CREATE TABLE sink_cdc (\n"
                                + "  order_id INT,\n"
                                + "  product_name STRING\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO sink_cdc\n"
                                + "SELECT o.id, p.name\n"
                                + "FROM orders o JOIN LATERAL SNAPSHOT(\n"
                                + "  input => TABLE products_cdc,\n"
                                + "  load_completed_condition => 'on_time',\n"
                                + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                + ") AS p\n"
                                + "ON o.product_id = p.id")
                .await();

        // Probe id=101 (product 1) sees the post-update value "A2", not "A". Probe id=102 sees
        // "B". Probes id=103 (product 3, no row) and id=104 (product 99, no row) match nothing.
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_cdc"))
                .containsExactlyInAnyOrder("+I[101, A2]", "+I[102, B]");
    }

    @Test
    void leftOuterJoinNullPadsUnmatchedProbe() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink (\n"
                                + "  order_id INT,\n"
                                + "  product_name STRING\n"
                                + ") WITH ('connector' = 'values')");
        tEnv().executeSql(
                        "INSERT INTO sink\n"
                                + "SELECT o.id, p.name\n"
                                + "FROM orders o LEFT JOIN LATERAL SNAPSHOT(\n"
                                + "  input => TABLE products,\n"
                                + "  load_completed_condition => 'on_time',\n"
                                + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                + ") AS p\n"
                                + "ON o.product_id = p.id")
                .await();

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder(
                        "+I[101, A]", "+I[102, B]", "+I[103, C]", "+I[104, null]");
    }
}
