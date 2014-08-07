/**
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

package org.apache.flink.compiler.dag;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.operators.base.CustomSortGroupReduceOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.dataproperties.GlobalProperties;
import org.apache.flink.compiler.dataproperties.LocalProperties;
import org.apache.flink.compiler.dataproperties.RequestedGlobalProperties;
import org.apache.flink.compiler.dataproperties.RequestedLocalProperties;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 */
public class CustomSortGroupReduceNode extends SingleInputNode {
	
	private CustomSortGroupReduceNode combinerUtilityNode;
	
	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract The reduce contract object.
	 */
	public CustomSortGroupReduceNode(CustomSortGroupReduceOperatorBase<?, ?, ?> pactContract) {
		super(pactContract);
		
		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
		}
	}
	
	public CustomSortGroupReduceNode(CustomSortGroupReduceNode reducerToCopyForCombiner) {
		super(reducerToCopyForCombiner);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	@Override
	public CustomSortGroupReduceOperatorBase<?, ?, ?> getPactContract() {
		return (CustomSortGroupReduceOperatorBase<?, ?, ?>) super.getPactContract();
	}

	/**
	 * Checks, whether a combiner function has been given for the function encapsulated
	 * by this reduce contract.
	 * 
	 * @return True, if a combiner has been given, false otherwise.
	 */
	public boolean isCombineable() {
		return getPactContract().isCombinable();
	}

	@Override
	public String getName() {
		return "GroupReduce";
	}
	
	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new CustomSortGroupReduceDescriptor());
	}
	
	// --------------------------------------------------------------------------------------------
	//  Estimates
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// no real estimates possible for a reducer.
	}
	
	public CustomSortGroupReduceNode getCombinerUtilityNode() {
		if (this.combinerUtilityNode == null) {
			this.combinerUtilityNode = new CustomSortGroupReduceNode(this);
			
			// we conservatively assume the combiner returns the same data size as it consumes 
			this.combinerUtilityNode.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
			this.combinerUtilityNode.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		}
		return this.combinerUtilityNode;
	}
	
	
	public static class CustomSortGroupReduceDescriptor extends OperatorDescriptorSingle {

		@Override
		public DriverStrategy getStrategy() {
			return DriverStrategy.CUSTOM_SORT_GROUP_REDUCE;
		}

		@Override
		public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
			return new SingleInputPlanNode(node, "CustomSortGroupReduce ("+node.getPactContract().getName()+")", in, DriverStrategy.CUSTOM_SORT_GROUP_REDUCE);
		}

		@Override
		protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
			RequestedGlobalProperties rgp = new RequestedGlobalProperties();
			rgp.setAnyPartitioning(new FieldSet(0));
			
			return Collections.singletonList(rgp);
		}

		@Override
		protected List<RequestedLocalProperties> createPossibleLocalProperties() {
			return Collections.singletonList(new RequestedLocalProperties());
		}
		
		@Override
		public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
			// TODO: for now remove all
			return new GlobalProperties();
		}
		
		@Override
		public LocalProperties computeLocalProperties(LocalProperties lProps) {
			// TODO: for now remove all
			return new LocalProperties();
		}
	}
}
