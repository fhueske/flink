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

package org.apache.flink.runtime.operators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.KeyGroupedIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * GroupReduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a GroupReduceFunction
 * implementation.
 * <p>
 * The GroupReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the GroupReduceFunction.
 * 
 * @see GenericGroupReduce
 */
public class CustomSortGroupReduceDriver<IT, OT> implements PactDriver<GroupReduceFunction<IT, OT>, OT> {
	
	private static final Log LOG = LogFactory.getLog(CustomSortGroupReduceDriver.class);

	private PactTaskContext<GroupReduceFunction<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;

	private TypeComparator<IT> sortComparator;
	
	private TypeComparator<IT> groupComparator;
	
	private UnilateralSortMerger<IT> sorter;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GroupReduceFunction<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GroupReduceFunction<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GroupReduceFunction<IT, OT>> clazz = (Class<GroupReduceFunction<IT, OT>>) (Class<?>) GroupReduceFunction.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.CUSTOM_SORT_GROUP_REDUCE) {
			throw new Exception("Unrecognized driver strategy for CustomSortGroupReduce driver: " + config.getDriverStrategy().name());
		}

		ClassLoader cl = ((RegularPactTask)this.taskContext.getOwningNepheleTask()).getUserCodeClassLoader();
				
		this.sortComparator = ((TypeComparatorFactory<IT>)config.getDriverComparator(0, cl)).createComparator();
		this.groupComparator = ((TypeComparatorFactory<IT>)config.getDriverComparator(1, cl)).createComparator();
				
		this.input = this.taskContext.getInput(0);
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		TypeSerializerFactory<IT> serializerFactory = this.taskContext.<IT>getInputSerializer(0);
				
		// obtain task manager's memory manager and I/O manager
		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();
		
		this.sorter = new UnilateralSortMerger<IT>(memoryManager, ioManager, 
			this.input, this.taskContext.getOwningNepheleTask(), serializerFactory, this.sortComparator,
			config.getRelativeMemoryDriver(), config.getFilehandlesDriver(), config.getSpillingThresholdDriver());
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("GroupReducer preprocessing done. Running GroupReducer code."));
		}

		final KeyGroupedIterator<IT> iter = new KeyGroupedIterator<IT>(this.sorter.getIterator(), this.serializer, this.groupComparator);

		// cache references on the stack
		final GroupReduceFunction<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		// run stub implementation
		while (this.running && iter.nextKey()) {
			stub.reduce(iter.getValues(), output);
		}
	}

	@Override
	public void cleanup() {
		
		if (this.sorter != null) {
			this.sorter.close();
			this.sorter = null;
		}

	}

	@Override
	public void cancel() {
		
		if (this.sorter != null) {
			this.sorter.close();
			this.sorter = null;
		}
		
		this.running = false;
	}
	
}