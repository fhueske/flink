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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Utilities for the LATERAL SNAPSHOT processing-time temporal join.
 *
 * <p>Provides:
 *
 * <ul>
 *   <li>A marker {@link SqlFunction} ({@code __LATERAL_SNAPSHOT_JOIN_CONDITION}) that the logical
 *       rule attaches to a {@code LogicalJoin}'s condition to carry the SNAPSHOT-specific arguments
 *       (load_completed_time, load_completed_idle_timeout, state_ttl) until the physical conversion
 *       phase.
 *   <li>Builders and extractors for that marker.
 *   <li>An {@link #isSnapshotFunction(FunctionDefinition)} check used by other physical rules to
 *       skip SNAPSHOT calls (e.g. the regular PROCESS_TABLE function rule).
 * </ul>
 */
@Internal
public final class LateralSnapshotJoinUtil {

    /**
     * Marker condition that wraps the SNAPSHOT-specific arguments. Operands (positional):
     *
     * <ol>
     *   <li>load_completed_time as {@code BIGINT} (millis since epoch) or {@code NULL}.
     *   <li>load_completed_idle_timeout as {@code BIGINT} (millis) or {@code NULL}.
     *   <li>state_ttl as {@code BIGINT} (millis) or {@code NULL}.
     * </ol>
     *
     * <p>The marker is added to the join condition so it survives logical optimization (filter
     * pushdown, predicate inference). The physical rule extracts the marker out of the condition
     * and reads the SNAPSHOT params.
     */
    public static final SqlFunction LATERAL_SNAPSHOT_JOIN_CONDITION =
            new SqlFunction(
                    "__LATERAL_SNAPSHOT_JOIN_CONDITION",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.sequence(
                            "'(LOAD_COMPLETED_TIME, LOAD_COMPLETED_IDLE_TIMEOUT, STATE_TTL)'",
                            OperandTypes.ANY,
                            OperandTypes.ANY,
                            OperandTypes.ANY),
                    SqlFunctionCategory.SYSTEM);

    /**
     * Builds the marker call for a {@link LogicalJoin} condition. {@code null} arguments are
     * encoded as a {@code BIGINT NULL} literal so the call's operand types stay numeric.
     */
    public static RexNode makeLateralSnapshotJoinConditionCall(
            RexBuilder rexBuilder,
            @Nullable Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs) {
        return rexBuilder.makeCall(
                LATERAL_SNAPSHOT_JOIN_CONDITION,
                makeBigIntOrNullLiteral(rexBuilder, loadCompletedTime),
                makeBigIntOrNullLiteral(rexBuilder, loadCompletedIdleTimeoutMs),
                makeBigIntOrNullLiteral(rexBuilder, stateTtlMs));
    }

    /**
     * Returns {@code true} if {@code condition} contains a {@link #LATERAL_SNAPSHOT_JOIN_CONDITION}
     * marker call anywhere in its expression tree.
     */
    public static boolean containsLateralSnapshotJoinCondition(@Nullable RexNode condition) {
        if (condition == null) {
            return false;
        }
        final boolean[] found = {false};
        condition.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if (call.getOperator() == LATERAL_SNAPSHOT_JOIN_CONDITION) {
                            found[0] = true;
                            return null;
                        }
                        return super.visitCall(call);
                    }
                });
        return found[0];
    }

    /**
     * Extracts the SNAPSHOT parameters from the marker inside {@code condition}. The marker must be
     * present (verified beforehand via {@link #containsLateralSnapshotJoinCondition}).
     */
    public static SnapshotJoinArgs extractSnapshotArgs(RexNode condition) {
        Objects.requireNonNull(condition);
        final RexCall[] markerHolder = new RexCall[1];
        condition.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if (call.getOperator() == LATERAL_SNAPSHOT_JOIN_CONDITION
                                && markerHolder[0] == null) {
                            markerHolder[0] = call;
                            return null;
                        }
                        return super.visitCall(call);
                    }
                });
        if (markerHolder[0] == null) {
            throw new IllegalStateException(
                    "Expected a LATERAL_SNAPSHOT_JOIN_CONDITION marker in: " + condition);
        }
        final RexCall marker = markerHolder[0];
        return new SnapshotJoinArgs(
                readBigIntLiteralOrNull(marker.getOperands().get(0)),
                readBigIntLiteralOrNull(marker.getOperands().get(1)),
                readBigIntLiteralOrNull(marker.getOperands().get(2)));
    }

    /**
     * Removes the marker from {@code condition} wherever it appears in the expression tree, not
     * only at top-level. The remaining condition is the join's actual predicate (e.g. the user's
     * {@code ON} clause). Top-level conjuncts that ARE the marker are dropped; markers nested
     * inside non-AND calls (which should not occur in practice but is defensive against future
     * predicate rewrites) are replaced with a {@code TRUE} literal so the surrounding expression
     * remains type-correct. The post-condition is verified with an assertion.
     */
    public static RexNode removeMarker(RexBuilder rexBuilder, RexNode condition) {
        final java.util.List<RexNode> conjuncts = RexUtil.flattenAnd(java.util.List.of(condition));
        final java.util.List<RexNode> survivors = new java.util.ArrayList<>(conjuncts.size());
        final RexNode trueLiteral = rexBuilder.makeLiteral(true);
        final RexShuttle nestedMarkerToTrue =
                new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (call.getOperator() == LATERAL_SNAPSHOT_JOIN_CONDITION) {
                            return trueLiteral;
                        }
                        return super.visitCall(call);
                    }
                };
        for (RexNode c : conjuncts) {
            if (c instanceof RexCall
                    && ((RexCall) c).getOperator() == LATERAL_SNAPSHOT_JOIN_CONDITION) {
                continue;
            }
            survivors.add(c.accept(nestedMarkerToTrue));
        }
        final RexNode result = RexUtil.composeConjunction(rexBuilder, survivors, false);
        if (containsLateralSnapshotJoinCondition(result)) {
            throw new IllegalStateException(
                    "removeMarker left a marker in the condition: " + result);
        }
        return result;
    }

    /**
     * {@code true} when {@code definition} is the {@link BuiltInFunctionDefinitions#SNAPSHOT}
     * built-in. Used by other rules to skip SNAPSHOT calls.
     */
    public static boolean isSnapshotFunction(@Nullable FunctionDefinition definition) {
        return definition instanceof BuiltInFunctionDefinition
                && BuiltInFunctionDefinitions.SNAPSHOT
                        .getName()
                        .equals(((BuiltInFunctionDefinition) definition).getName());
    }

    /**
     * {@code true} when the operator of {@code call} is a {@link BridgingSqlFunction} whose
     * function definition is the SNAPSHOT built-in.
     */
    public static boolean isSnapshotCall(@Nullable RexCall call) {
        if (call == null) {
            return false;
        }
        if (!(call.getOperator() instanceof BridgingSqlFunction)) {
            return false;
        }
        final BridgingSqlFunction bridging = (BridgingSqlFunction) call.getOperator();
        return isSnapshotFunction(bridging.getDefinition());
    }

    private static RexNode makeBigIntOrNullLiteral(RexBuilder rexBuilder, @Nullable Long value) {
        if (value == null) {
            return rexBuilder.makeNullLiteral(
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
        }
        return rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(value));
    }

    @Nullable
    private static Long readBigIntLiteralOrNull(RexNode node) {
        if (node instanceof RexLiteral) {
            final RexLiteral lit = (RexLiteral) node;
            if (lit.isNull()) {
                return null;
            }
            return lit.getValueAs(Long.class);
        }
        throw new IllegalStateException(
                "Expected a BIGINT literal in the LATERAL_SNAPSHOT_JOIN_CONDITION marker, but got: "
                        + node);
    }

    /** Snapshot-side parameters extracted from the marker. */
    public static final class SnapshotJoinArgs {

        @Nullable private final Long loadCompletedTime;
        @Nullable private final Long loadCompletedIdleTimeoutMs;
        @Nullable private final Long stateTtlMs;

        SnapshotJoinArgs(
                @Nullable Long loadCompletedTime,
                @Nullable Long loadCompletedIdleTimeoutMs,
                @Nullable Long stateTtlMs) {
            this.loadCompletedTime = loadCompletedTime;
            this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
            this.stateTtlMs = stateTtlMs;
        }

        @Nullable
        public Long getLoadCompletedTime() {
            return loadCompletedTime;
        }

        @Nullable
        public Long getLoadCompletedIdleTimeoutMs() {
            return loadCompletedIdleTimeoutMs;
        }

        @Nullable
        public Long getStateTtlMs() {
            return stateTtlMs;
        }
    }

    private LateralSnapshotJoinUtil() {}
}
