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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.StatusReporter;

	/**
	 * The reporter class of Flink.
	*/
	public class HadoopReporter extends StatusReporter implements Reporter {

	private transient Counters counters;

	private static final Log LOG = LogFactory.getLog(HadoopDummyProgressable.class);

	public HadoopReporter() {}

	public HadoopReporter(RuntimeContext context) {
		init(context);
	}

	public void init(RuntimeContext context) {
		this.counters = new FlinkHadoopCounters(context);
	}

	@Override
	public void progress() {
		LOG.warn("There is no need to report progress for Flink. progress() calls will be ignored.");
	}


	@Override
	public Counter getCounter(Enum<?> name) {
		return counters == null ? null : counters.findCounter(name);
	}

	@Override
	public Counter getCounter(String group, String name) {
		Counters.Counter counter = null;
		if (counters != null) {
			counter = counters.findCounter(group, name);
		}
		return counter;
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) {
		if (counters != null) {
			counters.findCounter(key).increment(amount);
		}
	}

	@Override
	public void incrCounter(String group, String counter, long amount) {
		if (counters != null) {
			counters.incrCounter(group, counter, amount);
		}
	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	// There should be an @Override, but some CDH4 dependency does not contain this method
	public float getProgress() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setStatus(String status) {}
}
