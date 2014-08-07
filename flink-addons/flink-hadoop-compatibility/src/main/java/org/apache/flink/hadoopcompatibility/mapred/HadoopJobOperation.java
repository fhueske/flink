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

package org.apache.flink.hadoopcompatibility.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.CustomSortGroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

public class HadoopJobOperation implements CustomUnaryOperation<Tuple2<Writable, Writable>, Tuple2<Writable, Writable>> {

	private static final Log LOG = LogFactory.getLog(HadoopJobOperation.class);
	private DataSet<Tuple2<Writable, Writable>> input;
	
	private final JobConf hadoopJobConf;
	private final int mapParallelism;
	private final int reduceParallelism;
	
	public HadoopJobOperation(JobConf hadoopJobConf, int mapDOP, int reduceDOP) {
		this.hadoopJobConf = hadoopJobConf;
		this.mapParallelism = mapDOP;
		this.reduceParallelism = reduceDOP;
	}
	
	@Override
	public void setInput(DataSet<Tuple2<Writable, Writable>> inputData) {
		this.input = inputData;	
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public DataSet<Tuple2<Writable, Writable>> createResult() {

		// apply Hadoop map function
		final FlatMapOperator<Tuple2<Writable,Writable>, Tuple2<Writable,Writable>> mapped = 
				new FlatMapOperator<Tuple2<Writable,Writable>, Tuple2<Writable,Writable>>(input, new HadoopMapFunction(hadoopJobConf));
		mapped.setParallelism(mapParallelism);
		mapped.name("Hadoop Mapper");
		
		if (reduceParallelism == 0) {
			return mapped;
		} else {
			
			// TODO add support for combiner
			if(hadoopJobConf.getCombinerClass() != null) {
				LOG.warn("Combine is not yet supported.");
			}
			
			// apply Hadoop reduce function
			final CustomSortGroupReduceOperator<Tuple2<Writable, Writable>, Tuple2<Writable,Writable>> reduced = 
					new CustomSortGroupReduceOperator<Tuple2<Writable, Writable>, Tuple2<Writable, Writable>>(mapped, new HadoopReduceFunction(hadoopJobConf));
			reduced.setParallelism(reduceParallelism);
			reduced.name("Hadoop Reducer");
			
			return reduced;
		}
				
	}
	
}
