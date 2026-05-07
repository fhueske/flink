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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;
import org.apache.flink.table.types.inference.strategies.LateralSnapshotTypeStrategy;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a {@link FlinkLogicalJoin} whose right side is a {@link FlinkLogicalTableFunctionScan}
 * backed by the {@code SNAPSHOT} built-in. The SNAPSHOT-specific arguments (load_completed_time,
 * load_completed_idle_timeout, state_ttl) are encoded into the join condition via {@link
 * LateralSnapshotJoinUtil#LATERAL_SNAPSHOT_JOIN_CONDITION} so they survive subsequent logical
 * optimization. The right-side input becomes the actual TABLE argument of the SNAPSHOT call (since
 * SNAPSHOT only passes through the input table's rows). A later physical rule converts the
 * marker-bearing join into the dedicated {@code StreamPhysicalLateralSnapshotJoin}.
 *
 * <p>By the time this rule fires, Calcite's decorrelator has already converted the original {@code
 * LogicalCorrelate} into a {@code LogicalJoin} (because SNAPSHOT does not actually reference any
 * field of the outer input). The rule therefore matches the join shape directly.
 */
@Value.Enclosing
public class LogicalCorrelateToJoinFromLateralSnapshotRule
        extends RelRule<
                LogicalCorrelateToJoinFromLateralSnapshotRule
                        .LogicalCorrelateToJoinFromLateralSnapshotRuleConfig> {

    public static final LogicalCorrelateToJoinFromLateralSnapshotRule INSTANCE =
            LogicalCorrelateToJoinFromLateralSnapshotRule
                    .LogicalCorrelateToJoinFromLateralSnapshotRuleConfig.DEFAULT
                    .toRule();

    private LogicalCorrelateToJoinFromLateralSnapshotRule(
            LogicalCorrelateToJoinFromLateralSnapshotRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        if (LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(join.getCondition())) {
            // Already rewritten — avoid infinite firing.
            return false;
        }
        return findSnapshotScan(join.getRight()) != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        final RelNode leftNode = join.getLeft();
        final FlinkLogicalTableFunctionScan scan = findSnapshotScan(join.getRight());
        if (scan == null) {
            return;
        }

        // Reject unsupported join types up front so the user sees a precise error instead of
        // the generic "SNAPSHOT can only be used inside a LATERAL clause" emitted by the orphan
        // rule when the rewrite is skipped. SQL syntax already restricts LATERAL-side joins to
        // INNER/LEFT, so this is a defensive check.
        final JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT join only supports INNER JOIN and LEFT OUTER JOIN, but was "
                            + joinType
                            + " JOIN.");
        }

        final RexCall snapshotCall = (RexCall) scan.getCall();

        // Locate the TABLE argument (RexTableArgCall at position 0 by SNAPSHOT signature).
        final RexNode inputArg =
                snapshotCall.getOperands().get(LateralSnapshotTypeStrategy.INPUT_ARG_INDEX);
        if (!(inputArg instanceof RexTableArgCall)) {
            return;
        }
        final RexTableArgCall tableArg = (RexTableArgCall) inputArg;
        final List<RelNode> scanInputs = scan.getInputs();
        if (tableArg.getInputIndex() < 0 || tableArg.getInputIndex() >= scanInputs.size()) {
            return;
        }
        // The build-side input must declare a watermark, otherwise the operator cannot
        // determine when the LOAD phase is complete. We check on the raw TABLE input to the
        // SNAPSHOT scan since any Calc above the scan may have projected the rowtime field away
        // (when the outer query doesn't reference it).
        final RelNode rawTableInput = unwrap(scanInputs.get(tableArg.getInputIndex()));
        boolean hasRowtime = false;
        for (int i = 0; i < rawTableInput.getRowType().getFieldCount(); i++) {
            if (FlinkTypeFactory.isRowtimeIndicatorType(
                    rawTableInput.getRowType().getFieldList().get(i).getType())) {
                hasRowtime = true;
                break;
            }
        }
        if (!hasRowtime) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT requires a watermark on the build-side input.");
        }

        // Replace the SNAPSHOT TableFunctionScan with its input, preserving any FlinkLogicalCalc
        // nodes that the optimizer placed above the scan (they carry projections that align the
        // scan's row type with the join's expected schema, e.g. casting away rowtime).
        final RelNode rightNode = replaceSnapshotScan(join.getRight());
        if (rightNode == null) {
            return;
        }

        final List<RexNode> operands = snapshotCall.getOperands();

        // Resolve load_completed_time according to load_completed_condition. Default
        // 'on_compile_time' uses the wall-clock time at planning.
        final String condition =
                readOptionalStringLiteral(
                        operands, LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_ARG_INDEX);
        final Long loadCompletedTime;
        if (condition == null
                || LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_ON_COMPILE_TIME.equals(
                        condition)) {
            loadCompletedTime = System.currentTimeMillis();
        } else if (LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_ON_TIME.equals(condition)) {
            loadCompletedTime =
                    readTimestampLiteralMillis(
                            operands, LateralSnapshotTypeStrategy.LOAD_COMPLETED_TIME_ARG_INDEX);
            if (loadCompletedTime == null) {
                throw new ValidationException(
                        "SNAPSHOT requires 'load_completed_time' when "
                                + "'load_completed_condition' is 'on_time'.");
            }
        } else {
            throw new ValidationException(
                    "Unknown SNAPSHOT 'load_completed_condition': '" + condition + "'.");
        }

        final Long loadCompletedIdleTimeoutMs =
                readIntervalMillis(
                        operands,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX);
        final Long stateTtlMs =
                readIntervalMillis(operands, LateralSnapshotTypeStrategy.STATE_TTL_ARG_INDEX);

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexNode marker =
                LateralSnapshotJoinUtil.makeLateralSnapshotJoinConditionCall(
                        rexBuilder, loadCompletedTime, loadCompletedIdleTimeoutMs, stateTtlMs);

        // Combine the existing ON predicate with the marker.
        final List<RexNode> conjuncts =
                new ArrayList<>(RexUtil.flattenAnd(List.of(join.getCondition())));
        conjuncts.add(marker);
        final RexNode newCondition = RexUtil.composeConjunction(rexBuilder, conjuncts, false);

        final RelNode newJoin =
                FlinkLogicalJoin.create(
                        leftNode, rightNode, newCondition, join.getHints(), joinType);
        call.transformTo(newJoin);
    }

    /**
     * Walks down a join's right input looking for a {@link FlinkLogicalTableFunctionScan} whose
     * call is the {@code SNAPSHOT} built-in. Returns {@code null} if no such scan exists at the top
     * of the right subtree (we only consider direct chains; an arbitrary tree shape would indicate
     * a query the rule shouldn't rewrite).
     */
    @Nullable
    private static FlinkLogicalTableFunctionScan findSnapshotScan(RelNode root) {
        RelNode current = unwrap(root);
        while (current != null) {
            if (current instanceof FlinkLogicalTableFunctionScan) {
                final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) current;
                if (scan.getCall() instanceof RexCall
                        && LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall())) {
                    return scan;
                }
                return null;
            }
            // Walk through pass-through nodes (e.g. FlinkLogicalCalc inserted by the optimizer).
            if (current instanceof FlinkLogicalCalc && current.getInputs().size() == 1) {
                current = unwrap(current.getInput(0));
            } else {
                return null;
            }
        }
        return null;
    }

    private static RelNode unwrap(RelNode node) {
        return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
    }

    /**
     * Walks the right subtree replacing the {@link FlinkLogicalTableFunctionScan} (the SNAPSHOT
     * scan) with the scan's TABLE input, while preserving any {@link FlinkLogicalCalc} nodes
     * stacked above the scan. The scan's row type passes through the input's row type unchanged
     * (per the SNAPSHOT type strategy), so each preserved Calc's RexProgram still resolves
     * correctly against the new input.
     */
    @Nullable
    private static RelNode replaceSnapshotScan(RelNode node) {
        final RelNode current = unwrap(node);
        if (current instanceof FlinkLogicalTableFunctionScan) {
            final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) current;
            if (!(scan.getCall() instanceof RexCall)
                    || !LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall())) {
                return null;
            }
            final RexCall snapshotCall = (RexCall) scan.getCall();
            final RexNode inputArg =
                    snapshotCall.getOperands().get(LateralSnapshotTypeStrategy.INPUT_ARG_INDEX);
            if (!(inputArg instanceof RexTableArgCall)) {
                return null;
            }
            final RexTableArgCall tableArg = (RexTableArgCall) inputArg;
            if (tableArg.getInputIndex() < 0
                    || tableArg.getInputIndex() >= scan.getInputs().size()) {
                return null;
            }
            return unwrap(scan.getInputs().get(tableArg.getInputIndex()));
        }
        if (current instanceof FlinkLogicalCalc && current.getInputs().size() == 1) {
            final RelNode rewrittenInput = replaceSnapshotScan(current.getInput(0));
            if (rewrittenInput == null) {
                return null;
            }
            return current.copy(current.getTraitSet(), List.of(rewrittenInput));
        }
        return null;
    }

    @Nullable
    private static String readOptionalStringLiteral(List<RexNode> operands, int index) {
        if (index >= operands.size()) {
            return null;
        }
        final RexNode operand = operands.get(index);
        if (!(operand instanceof RexLiteral)) {
            return null;
        }
        final RexLiteral literal = (RexLiteral) operand;
        if (literal.isNull()) {
            return null;
        }
        return literal.getValueAs(String.class);
    }

    @Nullable
    private static Long readTimestampLiteralMillis(List<RexNode> operands, int index) {
        if (index >= operands.size()) {
            return null;
        }
        final RexNode operand = operands.get(index);
        if (!(operand instanceof RexLiteral)) {
            return null;
        }
        final RexLiteral literal = (RexLiteral) operand;
        if (literal.isNull()) {
            return null;
        }
        // For TIMESTAMP_WITHOUT_TIME_ZONE literals Calcite returns the millis-since-epoch value
        // interpreted as UTC (no session-zone offset is applied). Build-side rowtime values are
        // also stored as UTC millis, so the operator's `latestBuildSideWm >= loadCompletedTime`
        // comparison against this value is consistent.
        return literal.getValueAs(Long.class);
    }

    @Nullable
    private static Long readIntervalMillis(List<RexNode> operands, int index) {
        if (index >= operands.size()) {
            return null;
        }
        final RexNode operand = operands.get(index);
        if (!(operand instanceof RexLiteral)) {
            return null;
        }
        final RexLiteral literal = (RexLiteral) operand;
        if (literal.isNull()) {
            return null;
        }
        final BigDecimal value = literal.getValueAs(BigDecimal.class);
        return value == null ? null : value.longValueExact();
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface LogicalCorrelateToJoinFromLateralSnapshotRuleConfig extends RelRule.Config {

        LogicalCorrelateToJoinFromLateralSnapshotRule
                        .LogicalCorrelateToJoinFromLateralSnapshotRuleConfig
                DEFAULT =
                        ImmutableLogicalCorrelateToJoinFromLateralSnapshotRule
                                .LogicalCorrelateToJoinFromLateralSnapshotRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 -> b0.operand(FlinkLogicalJoin.class).anyInputs())
                                .withDescription("LogicalCorrelateToJoinFromLateralSnapshotRule");

        @Override
        default LogicalCorrelateToJoinFromLateralSnapshotRule toRule() {
            return new LogicalCorrelateToJoinFromLateralSnapshotRule(this);
        }
    }
}
