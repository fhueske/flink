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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.immutables.value.Value;

import java.util.function.Function;

/**
 * Matches a {@link FlinkLogicalJoin} carrying the {@link
 * LateralSnapshotJoinUtil#LATERAL_SNAPSHOT_JOIN_CONDITION} marker (placed there by {@link
 * org.apache.flink.table.planner.plan.rules.logical.LogicalCorrelateToJoinFromLateralSnapshotRule})
 * and converts it to {@link StreamPhysicalLateralSnapshotJoin}.
 */
@Value.Enclosing
public class StreamPhysicalLateralSnapshotJoinRule
        extends StreamPhysicalJoinRuleBase<
                StreamPhysicalLateralSnapshotJoinRule.StreamPhysicalLateralSnapshotJoinRuleConfig> {

    public static final StreamPhysicalLateralSnapshotJoinRule INSTANCE =
            StreamPhysicalLateralSnapshotJoinRuleConfig.DEFAULT.toRule();

    public StreamPhysicalLateralSnapshotJoinRule(
            StreamPhysicalLateralSnapshotJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        if (!LateralSnapshotJoinUtil.containsLateralSnapshotJoinCondition(join.getCondition())) {
            return false;
        }
        final JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT join only supports INNER JOIN and LEFT OUTER JOIN, but was "
                            + joinType
                            + " JOIN.");
        }
        // Require at least one equality predicate so we can hash-partition the inputs.
        final JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT join requires at least one equality predicate.");
        }
        return true;
    }

    @Override
    public FlinkRelNode transform(
            FlinkLogicalJoin join,
            FlinkRelNode leftInput,
            Function<RelNode, RelNode> leftConversion,
            FlinkRelNode rightInput,
            Function<RelNode, RelNode> rightConversion,
            RelTraitSet providedTraitSet) {
        return new StreamPhysicalLateralSnapshotJoin(
                join.getCluster(),
                providedTraitSet,
                leftConversion.apply(leftInput),
                rightConversion.apply(rightInput),
                join.getCondition(),
                join.getJoinType());
    }

    /** Configuration. */
    @Value.Immutable
    public interface StreamPhysicalLateralSnapshotJoinRuleConfig
            extends StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig {

        StreamPhysicalLateralSnapshotJoinRuleConfig DEFAULT =
                ImmutableStreamPhysicalLateralSnapshotJoinRule
                        .StreamPhysicalLateralSnapshotJoinRuleConfig.builder()
                        .build()
                        .withOperandSupplier(OPERAND_TRANSFORM)
                        .withDescription("StreamPhysicalLateralSnapshotJoinRule")
                        .as(StreamPhysicalLateralSnapshotJoinRuleConfig.class);

        @Override
        default StreamPhysicalLateralSnapshotJoinRule toRule() {
            return new StreamPhysicalLateralSnapshotJoinRule(this);
        }
    }
}
