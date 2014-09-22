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

import java.io.IOException;
import java.util.Comparator;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.esotericsoftware.kryo.Kryo;

public class HadoopComparatorWrapper<K extends WritableComparable<?>, V extends Writable> extends TypeComparator<Tuple2<Integer,Tuple2<K,V>>> {
		
	private static final long serialVersionUID = 1L;
	
	private Class<K> type;
	
	private Class<Comparator<K>> comparatorType;
	
	private transient Comparator<K> writableComparator;
	
	private transient K reference;
	
	private transient K tempReference;
	
	private transient final Object[] extractedKey = new Object[1];
	
	@SuppressWarnings("unchecked")
	private transient final TypeComparator<Tuple2<Integer, Tuple2<K, V>>>[] comparators = 
			(TypeComparator<Tuple2<Integer, Tuple2<K, V>>>[]) new TypeComparator[] {this};
	
	private transient Kryo kryo;

	public HadoopComparatorWrapper() { }
	
	public HadoopComparatorWrapper(Class<Comparator<K>> comparatorType, Class<K> type) {
		this.comparatorType = comparatorType;
		this.type = type;
		
		this.writableComparator = InstantiationUtil.instantiate(comparatorType); 
	}
	
	@Override
	public Object[] extractKeys(Tuple2<Integer, Tuple2<K, V>> record) {
		extractedKey[0] = record;
		return extractedKey;
	}

	@Override
	public TypeComparator<Tuple2<Integer, Tuple2<K, V>>>[] getComparators() {
		return comparators;
	}
	
	@Override
	public int hash(Tuple2<Integer,Tuple2<K,V>> record) {
		return record.f1.f0.hashCode();
	}
	
	@Override
	public void setReference(Tuple2<Integer,Tuple2<K,V>> toCompare) {
		checkKryoInitialized();
		reference = this.kryo.copy(toCompare.f1.f0);
	}
	
	@Override
	public boolean equalToReference(Tuple2<Integer,Tuple2<K,V>> candidate) {
		return writableComparator.compare(candidate.f1.f0, reference) == 0;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compareToReference(TypeComparator<Tuple2<Integer,Tuple2<K,V>>> referencedComparator) throws ClassCastException {
		final K otherRef = ((HadoopComparatorWrapper<K,V>) referencedComparator).reference;
		return this.writableComparator.compare(otherRef, reference);
	}
	
	@Override
	public int compare(Tuple2<Integer,Tuple2<K,V>> first, Tuple2<Integer,Tuple2<K,V>> second) throws ClassCastException {
		return this.writableComparator.compare(first.f1.f0, second.f1.f0);
	}
	
	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		ensureReferenceInstantiated();
		ensureTempReferenceInstantiated();
		
		// skip serialized integer. This might break if the serialization 
		//    logic of the TupleSerializer is changed.
		// TODO quite hacky...
		firstSource.skipBytesToRead(IntSerializer.INSTANCE.getLength());
		secondSource.skipBytesToRead(IntSerializer.INSTANCE.getLength());
		
		reference.readFields(firstSource);
		tempReference.readFields(secondSource);

		return writableComparator.compare(reference, tempReference);
	}

	
	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}
	
	@Override
	public int getNormalizeKeyLen() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void putNormalizedKey(Tuple2<Integer,Tuple2<K,V>> record, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean invertNormalizedKey() {
		return false;
	}
	
	@Override
	public TypeComparator<Tuple2<Integer,Tuple2<K,V>>> duplicate() {
		return new HadoopComparatorWrapper<K,V>(comparatorType, type);
	}
	
	// --------------------------------------------------------------------------------------------
	// unsupported normalization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}
	
	@Override
	public void writeWithKeyNormalization(Tuple2<Integer,Tuple2<K,V>> record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Tuple2<Integer,Tuple2<K,V>> readWithKeyDenormalization(Tuple2<Integer,Tuple2<K,V>> reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}
	
	private final void ensureReferenceInstantiated() {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
	
	private final void ensureTempReferenceInstantiated() {
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
	
	public static class HadoopComparatorFactory<K extends WritableComparable<?>, V extends Writable> implements TypeComparatorFactory<Tuple2<Integer,Tuple2<K,V>>> {

		private Class<K> type;
		private Class<Comparator<K>> comparatorType;
		
		public HadoopComparatorFactory() {}
		
		public HadoopComparatorFactory(Class<K> type, Class<Comparator<K>> comparatorType) {
			this.type = type;
			this.comparatorType = comparatorType;
		}
		
		@Override
		public void writeParametersToConfig(Configuration config) {
			
			config.setClass("hadoop.comparator.type", type);
			config.setClass("hadoop.comparator.comparatortype", comparatorType);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
			this.type = (Class<K>)config.getClass("hadoop.comparator.type", null);
			this.comparatorType = (Class<Comparator<K>>)config.getClass("hadoop.comparator.comparatortype", null);
		}

		@Override
		public TypeComparator<Tuple2<Integer,Tuple2<K,V>>> createComparator() {
			return new HadoopComparatorWrapper<K,V>(comparatorType, type);
		}
		
			
	}
		
}