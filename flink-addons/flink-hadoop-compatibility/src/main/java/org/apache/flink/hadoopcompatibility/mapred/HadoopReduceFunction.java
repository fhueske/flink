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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.TupleUnwrappingIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopComparatorWrapper;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopReporter;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;

/**
 * The wrapper for a Hadoop Reducer (mapred API). Parses a Hadoop JobConf object and initialises all operations related
 * reducers and combiners.
 */
public final class HadoopReduceFunction<KEYIN extends WritableComparable<?>, VALUEIN extends Writable,	KEYOUT extends WritableComparable<?>, VALUEOUT extends Writable>
	extends org.apache.flink.api.java.functions.RichCustomSortGroupReduceFunction<Tuple2<KEYIN,VALUEIN>,Tuple2<KEYOUT,VALUEOUT>> implements Serializable, ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>> {

	private static final long serialVersionUID = 1L;

	private JobConf jobConf;

	private transient Class<KEYOUT> keyOutClass;
	private transient Class<VALUEOUT> valueOutClass;
	private transient Class<KEYIN> keyinClass;
	private transient Class<VALUEIN> valueinClass;
	private transient Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
	private transient Reducer<KEYIN,VALUEIN,KEYIN,VALUEIN> combiner;
	private transient HadoopOutputCollector<KEYIN,VALUEIN> combineCollector;
	private transient HadoopOutputCollector<KEYOUT,VALUEOUT> reduceCollector;
	private transient HadoopReporter reporter;
	private transient ReducerTransformingIterator iterator;

	@SuppressWarnings("unchecked")
	public HadoopReduceFunction(JobConf jobConf) {
		this.keyOutClass = (Class<KEYOUT>) jobConf.getOutputKeyClass();
		this.valueOutClass = (Class<VALUEOUT>) jobConf.getOutputValueClass();
		this.jobConf = jobConf;
	}


	/**
	 * A wrapping iterator for an iterator of key-value pairs that can be used as an iterator of values.
	 */
	private final class ReducerTransformingIterator extends TupleUnwrappingIterator<VALUEIN,KEYIN>
			implements java.io.Serializable {

		private static final long serialVersionUID = 1L;
		private Iterator<Tuple2<KEYIN,VALUEIN>> iterator;
		private KEYIN key;
		private Tuple2<KEYIN,VALUEIN> first;

		/**
		 * Set the iterator to wrap.
		 * @param iterator iterator to wrap
		 */
		@Override()
		public void set(final Iterator<Tuple2<KEYIN,VALUEIN>> iterator) {
			this.iterator = iterator;
			if(this.hasNext()) {
				this.first = iterator.next();
				this.key = this.first.f0;
			}
		}

		@Override
		public boolean hasNext() {
			if(this.first != null) {
				return true;
			}
			return iterator.hasNext();
		}

		@Override
		public VALUEIN next() {
			if(this.first != null) {
				final VALUEIN val = this.first.f1;
				this.first = null;
				return val;
			}
			final Tuple2<KEYIN,VALUEIN> tuple = iterator.next();
			return tuple.f1;
		}

		private KEYIN getKey() {
			return WritableUtils.clone(this.key, jobConf);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.reporter = new HadoopReporter(getRuntimeContext());

		combineCollector = new HadoopOutputCollector<KEYIN, VALUEIN>(keyinClass, valueinClass);
		reduceCollector = new HadoopOutputCollector<KEYOUT, VALUEOUT>(keyOutClass, valueOutClass);
	}

	/**
	 * Wrap a hadoop reduce() function call and use a Flink collector to collect the result values.
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 *
	 * @throws Exception
	 */
	@Override
	public void reduce(final Iterable<Tuple2<KEYIN,VALUEIN>> values, final Collector<Tuple2<KEYOUT,VALUEOUT>> out)
			throws Exception {
		reduceCollector.set(out);
		iterator.set(values.iterator());
		reducer.reduce(iterator.getKey(), iterator, reduceCollector, reporter);
	}

	/**
	 * Wrap a hadoop combine() function call and use a Flink collector to collect the result values.
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 *
	 * @throws Exception
	 */
	@Override
	public void combine(final Iterable<Tuple2<KEYIN,VALUEIN>> values, final Collector<Tuple2<KEYIN,VALUEIN>> out)
			throws Exception {
		if (this.combiner == null) {
			throw new RuntimeException("No combiner has been specified in Hadoop job. Flink reduce function is" +
					"declared combinable. Invalid behaviour.");  //This should not happen.
		}
		else {
			combineCollector.set(out);
			iterator.set(values.iterator());
			combiner.reduce(iterator.getKey(), iterator, combineCollector, reporter);
		}
	}

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyOutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valueOutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {
		HadoopConfiguration.writeHadoopJobConf(jobConf, out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.reducer = (Reducer) InstantiationUtil.instantiate(jobConf.getReducerClass());
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop reducer", e);
		}
		iterator = new ReducerTransformingIterator();

		keyOutClass = (Class<KEYOUT>) jobConf.getOutputKeyClass();
		valueOutClass = (Class<VALUEOUT>) jobConf.getOutputValueClass();

		keyinClass = (Class<KEYIN>) jobConf.getMapOutputKeyClass();
		valueinClass = (Class<VALUEIN>) jobConf.getMapOutputValueClass();

		final Class combinerClass = jobConf.getCombinerClass();
		if (combinerClass != null) {
			combiner = InstantiationUtil.instantiate(jobConf.getCombinerClass());
			combiner.configure(jobConf);
		}
		reducer.configure(jobConf);
		reducer = InstantiationUtil.instantiate(jobConf.getReducerClass());
		reducer.configure(jobConf);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <V> KeySelector<Tuple2<KEYIN, VALUEIN>, Integer> getCustomPartitionKeySelector() {
		final Class<Partitioner<KEYIN, VALUEIN>> partitionerClass = (Class<Partitioner<KEYIN, VALUEIN>>)jobConf.getPartitionerClass();
		return new HadoopKeySelector<KEYIN, VALUEIN>(partitionerClass);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeComparatorFactory<Tuple2<Integer,Tuple2<KEYIN, VALUEIN>>> getCustomSortComparatorFactory() {
		Comparator<KEYIN> comp = (Comparator<KEYIN>)jobConf.getOutputKeyComparator();
		return new HadoopComparatorWrapper.HadoopComparatorFactory<KEYIN, VALUEIN>((Class<KEYIN>)jobConf.getMapOutputKeyClass(), ((Class<Comparator<KEYIN>>)comp.getClass()));
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeComparatorFactory<Tuple2<Integer,Tuple2<KEYIN, VALUEIN>>> getCustomGroupingComparatorFactory() {
		Comparator<KEYIN> comp = (Comparator<KEYIN>)jobConf.getOutputValueGroupingComparator();
		return new HadoopComparatorWrapper.HadoopComparatorFactory<KEYIN, VALUEIN>((Class<KEYIN>)jobConf.getMapOutputKeyClass(), ((Class<Comparator<KEYIN>>)comp.getClass()));
	}
	
	public static class HadoopKeySelector<K, V> implements KeySelector<Tuple2<K,V>, Integer> {
		private static final long serialVersionUID = 1L;
		private transient Partitioner<K,V> partitioner;
		private Class<Partitioner<K,V>> partitionerClass;
		
		public HadoopKeySelector(Class<Partitioner<K,V>> partitionerClass) {
			this.partitionerClass = partitionerClass;
			this.partitioner = InstantiationUtil.instantiate(partitionerClass);
		}

		@Override
		public Integer getKey(Tuple2<K, V> value) {
			
			if(this.partitioner == null) {
				this.partitioner = InstantiationUtil.instantiate(partitionerClass);
			}
			
			return partitioner.getPartition(value.f0, value.f1, Integer.MAX_VALUE);
		}
	}
}
