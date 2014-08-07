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

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * The abstract base class for group reduce functions. Group reduce functions process groups of elements.
 * They may aggregate them to a single value, or produce multiple result values for each group.
 * <p>
 * For a reduce functions that works incrementally by combining always two elements, see 
 * {@link ReduceFunction}, called via {@link org.apache.flink.api.java.DataSet#reduce(ReduceFunction)}.
 * <p>
 * The basic syntax for using a grouped GroupReduceFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<X> result = input.groupBy(<key-definition>).reduceGroup(new MyGroupReduceFunction());
 * </blockquote></pre>
 * <p>
 * GroupReduceFunctions may be "combinable", in which case they can pre-reduce partial groups in order to
 * reduce the data volume early. See the {@link #combine(Iterator, Collector)} function for details.
 * <p>
 * Like all functions, the GroupReduceFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN> Type of the elements that this function processes.
 * @param <OUT> The type of the elements returned by the user-defined function.
 */
public abstract class RichCustomSortGroupReduceFunction<IN, OUT> extends AbstractRichFunction 
						implements GroupReduceFunction<IN, OUT>, FlatCombineFunction<IN> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Core method of the reduce function. It is called one per group of elements. If the reducer
	 * is not grouped, than the entire data set is considered one group.
	 * 
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract void reduce(Iterable<IN> values, Collector<OUT> out) throws Exception;
	
	// --------------------------------------------------------------------------------------------
	
	public abstract <V> KeySelector<IN, Integer> getCustomPartitionKeySelector();
		
	public abstract TypeComparatorFactory<Tuple2<Integer,IN>> getCustomSortComparatorFactory();
	
	public abstract TypeComparatorFactory<Tuple2<Integer,IN>> getCustomGroupingComparatorFactory();
	
}
