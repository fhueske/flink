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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.CustomSortGroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.RichCustomSortGroupReduceFunction;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.TupleUnwrappingIterator;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * This operator represents the application of a "reduceGroup" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class CustomSortGroupReduceOperator<IN, OUT> 
				extends SingleInputUdfOperator<IN, OUT, CustomSortGroupReduceOperator<IN, OUT>> {
	
	private final RichCustomSortGroupReduceFunction<IN, OUT> function;
	
	private boolean combinable = false;
	
	
	/**
	 * Constructor for a non-grouped reduce (all reduce).
	 * 
	 * @param input The input data set to the groupReduce function.
	 * @param function The user-defined GroupReduce function.
	 */
	public CustomSortGroupReduceOperator(DataSet<IN> input, RichCustomSortGroupReduceFunction<IN, OUT> function) {
		super(input, TypeExtractor.getGroupReduceReturnTypes(function, input.getType()));
		
		this.function = function;
		checkCombinability();
	}
	
	private void checkCombinability() {
		
		// TODO check if there is a combiner in the job conf
		
//		if (function instanceof GenericCombine && function.getClass().getAnnotation(Combinable.class) != null) {
//			this.combinable = true;
//		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public boolean isCombinable() {
		return combinable;
	}
	
	public void setCombinable(boolean combinable) {
		// sanity check that the function is a subclass of the combine interface
		//if (combinable && !(function instanceof FlatCombineFunction)) {
		//	throw new IllegalArgumentException("The function does not implement the combine interface.");
		//}
		
		//this.combinable = combinable;
		// TODO
		this.combinable = false;
	}
	
	@Override
	protected org.apache.flink.api.common.operators.base.CustomSortGroupReduceOperatorBase<Tuple2<Integer, IN>, OUT, ?> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		
		KeySelector<IN, Integer> ks = function.getCustomPartitionKeySelector();
		KeyExtractingMapper<IN, Integer> extractor = new KeyExtractingMapper<IN, Integer>(ks);
		
		TypeInformation<Tuple2<Integer, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple2<Integer, IN>>(BasicTypeInfo.INT_TYPE_INFO, getInputType());
		MapOperatorBase<IN, Tuple2<Integer, IN>, MapFunction<IN, Tuple2<Integer, IN>>> extractMap = 
				new MapOperatorBase<IN, Tuple2<Integer, IN>, MapFunction<IN, Tuple2<Integer, IN>>>(extractor, new UnaryOperatorInformation<IN, Tuple2<Integer, IN>>(getInputType(), typeInfoWithKey), "Key Extractor");

		CustomSortGroupReduceUnwrappingOperator<IN, OUT> reducer = new CustomSortGroupReduceUnwrappingOperator<IN, OUT>(function, name, getResultType(), typeInfoWithKey, combinable);
		
		reducer.setInput(extractMap);
		extractMap.setInput(input);
		
		// set the mapper's parallelism to the input parallelism to make sure it is chained
		extractMap.setDegreeOfParallelism(input.getDegreeOfParallelism());
		reducer.setDegreeOfParallelism(this.getParallelism());
		
		return reducer;
	}
	
	
	
	public class CustomSortGroupReduceUnwrappingOperator<IN, OUT> 
					extends CustomSortGroupReduceOperatorBase<Tuple2<Integer, IN>, OUT, GroupReduceFunction<Tuple2<Integer, IN>, OUT>> {

		public CustomSortGroupReduceUnwrappingOperator(RichCustomSortGroupReduceFunction<IN, OUT> udf, String name,
				TypeInformation<OUT> outType, TypeInformation<Tuple2<Integer, IN>> typeInfoWithKey, boolean combinable)
		{
			
			// TODO
//			super(combinable ? new TupleUnwrappingCombinableGroupReducer<IN, OUT, K>(udf) : new TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K>(udf),
//					new UnaryOperatorInTuple2<IN, K>formation<Tuple2<K, IN>, OUT>(typeInfoWithKey, outType), new int[]{0}, name);
			
			super(new TupleUnwrappingNonCombinableCustomSortGroupReducer<IN, OUT>(udf),
					new UnaryOperatorInformation<Tuple2<Integer, IN>, OUT>(typeInfoWithKey, outType), new int[]{0}, name);
			
			super.setCombinable(combinable);
			
		}
	}
		
		// --------------------------------------------------------------------------------------------
		
	public static final class TupleUnwrappingNonCombinableCustomSortGroupReducer<IN, OUT> 
		extends WrappingFunction<RichCustomSortGroupReduceFunction<IN, OUT>>
		implements GroupReduceFunction<Tuple2<Integer, IN>, OUT>
	{
	
		private static final long serialVersionUID = 1L;
		
		private final TupleUnwrappingIterator<IN, Integer> iter;
		
		private TupleUnwrappingNonCombinableCustomSortGroupReducer(RichCustomSortGroupReduceFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new TupleUnwrappingIterator<IN, Integer>();
		}
		
		@Override
		public void reduce(Iterable<Tuple2<Integer, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values.iterator());
			this.wrappedFunction.reduce(iter, out);
		}
		
		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
		
		public RichCustomSortGroupReduceFunction<IN, OUT> getCustomSortGroupReduceUDF() {
			return super.wrappedFunction;
		}
	}
		
}	

