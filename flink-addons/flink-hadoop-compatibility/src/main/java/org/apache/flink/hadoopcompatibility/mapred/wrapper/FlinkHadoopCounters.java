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


package org.apache.flink.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.mapred.Counters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Counters to be used by the hadoop-compatibility layer of Flink. The counters specified inside the Hadoop job are
 * translated to Flink accumulators and distributed by the framework
 */
public class FlinkHadoopCounters extends Counters {

	private Map<String, CounterWrappingAccumulator> accumulators;   //the accumulators collection -> (Name, Accumulator)
	private RuntimeContext context;

	public FlinkHadoopCounters(RuntimeContext context) {
		this.context = context;
		this.accumulators = new HashMap<String, CounterWrappingAccumulator>();
	}

	@SuppressWarnings("unchecked")
	public FlinkHadoopCounters(Map accumulators) {
		this.accumulators = accumulators;
	}

	@Override
	public synchronized Counter findCounter(String group, String name) {
		final Counter counter =  super.findCounter(group, name);
		registerCounter(name, counter);

		return counter;
	}

	@Override
	public synchronized Counter findCounter(Enum key) {
		final Counter counter =  super.findCounter(key);
		registerCounter(key.name(), counter);

		return counter;
	}

	@Override
	public synchronized void incrCounter(Enum key, long amount) {
		final Counter counter = findCounter(key);
		counter.increment(amount);
		registerCounter(key.name(), counter);
		accumulators.get(key.name()).add(amount);
	}

	@Override
	public synchronized void incrCounter(String group, String counter, long amount) {
		Group retGroup = getGroup(group);
		Counter retCounter = null;
		if (retGroup != null) {
			retCounter = retGroup.getCounterForName(counter);
			if (retCounter != null) {
				retCounter.increment(amount);
			}
		}
		super.incrCounter(group, counter, amount);
		registerCounter(counter, retCounter);
	}

	@Override
	public synchronized String toString() {
		final StringBuffer buffer = new StringBuffer();  // StringBuilder is not thread-safe.
		for (Accumulator accumulator : accumulators.values()) {
			buffer.append(accumulator.toString());
		}

		return buffer.toString();
	}

	/**
	 * When a Counter is created an Accumulator is registered to the colection of Accumulators.
	 * @param name name of the Counter / Accumulator
	 * @param counter the counter to wrap
	 */
	private void registerCounter(String name, Counter counter) {
		if (accumulators.get(counter.getName()) == null) {
			CounterWrappingAccumulator hadoopAccumulator = new CounterWrappingAccumulator(counter);
			context.addAccumulator(name, hadoopAccumulator);
			accumulators.put(name, hadoopAccumulator);
		}
	}

	/**
	 * A Flink accumulator that wraps a Hadoop Counter.
	 */
	private class CounterWrappingAccumulator implements Accumulator<Long, Long>{

		private final Counter counter;

		public CounterWrappingAccumulator(Counter counter) {
			this.counter = counter;
		}

		@Override
		public void add(final Long value) {
			counter.increment(value);
		}

		@Override
		public Long getLocalValue() {
			return this.counter.getValue();
		}

		@Override
		public void resetLocal() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void merge(final Accumulator<Long, Long> other) {
			this.add(other.getLocalValue());
		}

		@Override
		public void write(final DataOutputView out) throws IOException {
			counter.write(out);
		}

		@Override
		public void read(final DataInputView in) throws IOException {
			counter.readFields(in);
		}

		@Override
		public String toString() {
			return counter.getDisplayName() + " = " + counter.getValue();
		}
	}
}