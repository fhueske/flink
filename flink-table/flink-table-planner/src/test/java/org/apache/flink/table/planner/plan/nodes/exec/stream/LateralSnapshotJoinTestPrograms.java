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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecLateralSnapshotJoin}. */
public class LateralSnapshotJoinTestPrograms {

    /**
     * Build-side rows seen before the savepoint. The third row's timestamp (10s) is past the {@code
     * load_completed_time} of 5s and forces the LOAD→JOIN flip on the build-side watermark before
     * the savepoint is triggered. Without this, the flip would never happen and the sink would emit
     * nothing, blocking the savepoint indefinitely.
     */
    static final Row[] PRODUCTS_BEFORE = {
        Row.of(1, "A", "1970-01-01 00:00:01"),
        Row.of(2, "B", "1970-01-01 00:00:02"),
        Row.of(3, "C", "1970-01-01 00:00:10"),
    };

    /** Build-side rows seen after restore — a new product arriving in JOIN phase. */
    static final Row[] PRODUCTS_AFTER = {
        Row.of(4, "D", "1970-01-01 00:00:20"),
    };

    /** Probe-side rows produced before the savepoint. */
    static final Row[] ORDERS_BEFORE = {
        Row.of(101, 1, "1970-01-01 00:00:11"), Row.of(102, 2, "1970-01-01 00:00:12"),
    };

    /** Probe-side rows produced after restore. */
    static final Row[] ORDERS_AFTER = {
        Row.of(103, 3, "1970-01-01 00:00:13"), Row.of(104, 1, "1970-01-01 00:00:14"),
    };

    static final String[] PRODUCTS_SCHEMA = {
        "id INT",
        "name STRING",
        "ts_str STRING",
        "ts AS TO_TIMESTAMP(ts_str)",
        "WATERMARK FOR ts AS ts"
    };

    static final String[] ORDERS_SCHEMA = {
        "id INT",
        "product_id INT",
        "ts_str STRING",
        "ts AS TO_TIMESTAMP(ts_str)",
        "WATERMARK FOR ts AS ts"
    };

    static final String[] SINK_SCHEMA = {"order_id INT", "product_name STRING"};

    /** INNER JOIN with explicit on_time flip. */
    static final TableTestProgram LATERAL_SNAPSHOT_INNER_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-inner-join",
                            "validates lateral snapshot inner join across a savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("products")
                                    .addSchema(PRODUCTS_SCHEMA)
                                    .producedBeforeRestore(PRODUCTS_BEFORE)
                                    .producedAfterRestore(PRODUCTS_AFTER)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(ORDERS_SCHEMA)
                                    .producedBeforeRestore(ORDERS_BEFORE)
                                    .producedAfterRestore(ORDERS_AFTER)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore("+I[101, A]", "+I[102, B]")
                                    .consumedAfterRestore("+I[103, C]", "+I[104, A]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t\n"
                                    + "SELECT o.id, p.name\n"
                                    + "FROM orders o JOIN LATERAL SNAPSHOT(\n"
                                    + "  input => TABLE products,\n"
                                    + "  load_completed_condition => 'on_time',\n"
                                    + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                    + ") AS p\n"
                                    + "ON o.product_id = p.id")
                    .build();

    /** LEFT OUTER JOIN with explicit on_time flip; null-pads unmatched probes. */
    static final TableTestProgram LATERAL_SNAPSHOT_LEFT_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-left-join",
                            "validates lateral snapshot left outer join across a savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("products")
                                    .addSchema(PRODUCTS_SCHEMA)
                                    .producedBeforeRestore(PRODUCTS_BEFORE)
                                    .producedAfterRestore(PRODUCTS_AFTER)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(ORDERS_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of(101, 1, "1970-01-01 00:00:11"),
                                            Row.of(199, 99, "1970-01-01 00:00:11"))
                                    .producedAfterRestore(
                                            Row.of(103, 3, "1970-01-01 00:00:13"),
                                            Row.of(298, 88, "1970-01-01 00:00:13"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore("+I[101, A]", "+I[199, null]")
                                    .consumedAfterRestore("+I[103, C]", "+I[298, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t\n"
                                    + "SELECT o.id, p.name\n"
                                    + "FROM orders o LEFT JOIN LATERAL SNAPSHOT(\n"
                                    + "  input => TABLE products,\n"
                                    + "  load_completed_condition => 'on_time',\n"
                                    + "  load_completed_time => TIMESTAMP '1970-01-01 00:00:05'\n"
                                    + ") AS p\n"
                                    + "ON o.product_id = p.id")
                    .build();
}
