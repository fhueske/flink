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

import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

/**
 * Restore tests for {@link StreamExecLateralSnapshotJoin}.
 *
 * <p>The savepoint and compiled-plan resources under {@code
 * src/test/resources/restore-tests/stream-exec-lateral-snapshot-join_1/} must be regenerated
 * whenever the operator's state schema or the exec node's serialized form changes. Run {@code
 * RestoreTestBase#generateTestSetupFiles(TableTestProgram)} once (it is annotated
 * {@code @Disabled}) to produce them, then commit.
 */
public class LateralSnapshotJoinRestoreTest extends RestoreTestBase {

    public LateralSnapshotJoinRestoreTest() {
        super(StreamExecLateralSnapshotJoin.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                LateralSnapshotJoinTestPrograms.LATERAL_SNAPSHOT_INNER_JOIN,
                LateralSnapshotJoinTestPrograms.LATERAL_SNAPSHOT_LEFT_JOIN);
    }

    /**
     * Re-exposes {@link RestoreTestBase#generateTestSetupFiles(TableTestProgram)} without the
     * {@code @Disabled} annotation so it can be run from Maven by passing {@code
     * -DgenerateRestoreFiles=true}. The base method is final-by-convention but not actually final;
     * this override delegates to it.
     */
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @EnabledIfSystemProperty(named = "generateRestoreFiles", matches = "true")
    public void regenerateRestoreFiles(TableTestProgram program) throws Exception {
        super.generateTestSetupFiles(program);
    }
}
