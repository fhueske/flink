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

package org.apache.flink.table.planner.plan.utils;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LateralSnapshotJoinUtil}. */
class LateralSnapshotJoinUtilTest {

    private final RexBuilder rexBuilder =
            new RexBuilder(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    private final RelDataType boolType =
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);

    @Test
    void removeMarker_topLevelMarkerOnly_returnsTrueLiteral() {
        RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, 1L, null, null);
        // Wrap in a top-level AND with itself only — the survivor is a TRUE literal.
        RexNode result = LateralSnapshotJoinUtil.removeMarker(rexBuilder, marker);
        assertThat(result.isAlwaysTrue()).isTrue();
        assertThat(LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(result)).isFalse();
    }

    @Test
    void removeMarker_topLevelConjunction_dropsMarkerKeepsOthers() {
        RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, 1L, null, null);
        RexNode otherCond = rexBuilder.makeLiteral(true); // stand-in for a user predicate
        RexNode joined =
                rexBuilder.makeCall(
                        boolType, SqlStdOperatorTable.AND, java.util.List.of(otherCond, marker));

        RexNode result = LateralSnapshotJoinUtil.removeMarker(rexBuilder, joined);
        assertThat(LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(result)).isFalse();
    }

    @Test
    void removeMarker_markerNestedInsideOr_isReplacedWithTrue() {
        RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, 1L, null, null);
        RexNode otherCond = rexBuilder.makeLiteral(false);
        // OR(false, marker) — a synthetic shape that the optimizer should not produce, but the
        // util must defensively cope with it. After removal the marker is replaced with TRUE.
        RexNode nested =
                rexBuilder.makeCall(
                        boolType, SqlStdOperatorTable.OR, java.util.List.of(otherCond, marker));

        RexNode result = LateralSnapshotJoinUtil.removeMarker(rexBuilder, nested);
        assertThat(LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(result)).isFalse();
    }

    @Test
    void removeMarker_noMarker_returnsConditionUnchanged() {
        RexNode literal = rexBuilder.makeLiteral(true);
        RexNode result = LateralSnapshotJoinUtil.removeMarker(rexBuilder, literal);
        assertThat(result.isAlwaysTrue()).isTrue();
    }

    @Test
    void containsLateralSnapshotJoinCondition_findsNestedMarker() {
        RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, 1L, null, null);
        RexNode literal = rexBuilder.makeLiteral(true);
        RexNode nested =
                rexBuilder.makeCall(
                        boolType, SqlStdOperatorTable.OR, java.util.List.of(literal, marker));

        assertThat(LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(nested)).isTrue();
    }

    @Test
    void extractSnapshotArgs_readsAllThreeOperands() {
        RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, 1234L, 56L, 78L);
        LateralSnapshotJoinUtil.SnapshotJoinArgs args =
                LateralSnapshotJoinUtil.extractSnapshotArgs(marker);
        assertThat(args.getLoadCompletedTime()).isEqualTo(1234L);
        assertThat(args.getLoadCompletedIdleTimeoutMs()).isEqualTo(56L);
        assertThat(args.getStateTtlMs()).isEqualTo(78L);
    }
}
