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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.immutables.value.Value;

/**
 * Validation rule that rejects {@code SNAPSHOT(...)} calls used outside a {@code LATERAL} (i.e.
 * join) context with a clear error message.
 *
 * <p>This rule fires after {@link LogicalCorrelateToJoinFromLateralSnapshotRule} has rewritten all
 * legitimate {@code LATERAL SNAPSHOT(...)} uses (replacing the {@link
 * FlinkLogicalTableFunctionScan} with the scan's input). Any {@link FlinkLogicalTableFunctionScan}
 * backed by the SNAPSHOT built-in that survives until this rule fires is necessarily orphaned — the
 * user wrote {@code SNAPSHOT(...)} in a position the {@link
 * LogicalCorrelateToJoinFromLateralSnapshotRule} cannot handle (e.g. directly in {@code FROM}
 * without {@code LATERAL}, or in a join shape the rule does not recognize) and we want to fail the
 * query with an explicit message instead of falling through to a downstream "no physical
 * conversion" error.
 */
@Value.Enclosing
public class RejectOrphanLateralSnapshotScanRule
        extends RelRule<RejectOrphanLateralSnapshotScanRule.Config> {

    public static final RejectOrphanLateralSnapshotScanRule INSTANCE = Config.DEFAULT.toRule();

    private RejectOrphanLateralSnapshotScanRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        return scan.getCall() instanceof RexCall
                && LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new ValidationException(
                "SNAPSHOT can only be used inside a LATERAL clause, e.g. "
                        + "'FROM probe JOIN LATERAL SNAPSHOT(TABLE build) AS s ON probe.k = s.k'.");
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableRejectOrphanLateralSnapshotScanRule.Config.builder()
                        .build()
                        .withOperandSupplier(
                                b0 -> b0.operand(FlinkLogicalTableFunctionScan.class).anyInputs())
                        .withDescription("RejectOrphanLateralSnapshotScanRule");

        @Override
        default RejectOrphanLateralSnapshotScanRule toRule() {
            return new RejectOrphanLateralSnapshotScanRule(this);
        }
    }
}
