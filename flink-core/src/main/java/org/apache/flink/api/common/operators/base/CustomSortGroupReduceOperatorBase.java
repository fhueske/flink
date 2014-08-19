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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;

public class CustomSortGroupReduceOperatorBase<IN, OUT, FT extends GroupReduceFunction<IN, OUT>> extends SingleInputOperator<IN, OUT, FT> {

	private boolean combinable;
	
	
	public CustomSortGroupReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(udf, operatorInfo, keyPositions, name);
	}
	
	public CustomSortGroupReduceOperatorBase(FT udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	public CustomSortGroupReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	public CustomSortGroupReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public CustomSortGroupReduceOperatorBase(FT udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public CustomSortGroupReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Marks the group reduce operation as combinable. Combinable operations may pre-reduce the
	 * data before the actual group reduce operations. Combinable user-defined functions
	 * must implement the interface {@link GenericCombine}.
	 * 
	 * @param combinable Flag to mark the group reduce operation as combinable.
	 */
	public void setCombinable(boolean combinable) {
		// sanity check
		//if (combinable && !GenericCombine.class.isAssignableFrom(this.userFunction.getUserCodeClass())) {
		//	throw new IllegalArgumentException("Cannot set a UDF as combinable if it does not implement the interface " + 
		//			GenericCombine.class.getName());
		//} else {
		//	this.combinable = combinable;
		//}
		// TODO
		this.combinable = false;
	}
	
	/**
	 * Checks whether the operation is combinable.
	 * 
	 * @return True, if the UDF is combinable, false if not.
	 * 
	 * @see #setCombinable(boolean)
	 */
	public boolean isCombinable() {
		return this.combinable;
	}

}
