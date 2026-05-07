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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLateralSnapshotJoin
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.RexNode

/**
 * Stream physical node for the LATERAL SNAPSHOT processing-time temporal table join. The build side
 * is loaded into operator state during a LOAD phase; once the build-side watermark crosses the
 * configured flip point, the operator switches to a JOIN phase and processes probe-side records
 * against the loaded build state.
 */
class StreamPhysicalLateralSnapshotJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = true

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamPhysicalLateralSnapshotJoin(cluster, traitSet, left, right, conditionExpr, joinType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val rexBuilder = cluster.getRexBuilder
    val args = LateralSnapshotJoinUtil.extractSnapshotArgs(getCondition)

    // Strip the marker out of the non-equi condition so the join condition is just the user's
    // ON predicate (e.g. equality keys + any extra residual predicate).
    val nonEquiCondition = joinSpec.getNonEquiCondition.orElse(null)
    val cleanedNonEquiCondition =
      if (nonEquiCondition == null) null
      else {
        val survivor = LateralSnapshotJoinUtil.removeMarker(rexBuilder, nonEquiCondition)
        if (survivor.isAlwaysTrue) null else survivor
      }

    val cleanedJoinSpec = new JoinSpec(
      joinSpec.getJoinType,
      joinSpec.getLeftKeys,
      joinSpec.getRightKeys,
      joinSpec.getFilterNulls,
      cleanedNonEquiCondition)

    new StreamExecLateralSnapshotJoin(
      unwrapTableConfig(this),
      cleanedJoinSpec,
      args.getLoadCompletedTime,
      args.getLoadCompletedIdleTimeoutMs,
      args.getStateTtlMs,
      InputProperty.DEFAULT,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
