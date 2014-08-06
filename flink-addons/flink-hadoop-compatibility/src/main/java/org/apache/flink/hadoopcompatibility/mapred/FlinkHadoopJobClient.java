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


import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.InvalidTypesException;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.FlinkHadoopCounters;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopReporter;
import org.apache.flink.types.TypeInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The client to control Hadoop Jobs on Flink.
 */
public final class FlinkHadoopJobClient extends JobClient {

	private final static int TASK_SLOTS = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, -1);
	public static final int MAX_VALUE = 2147483647;
	private static final Log LOG = LogFactory.getLog(FlinkHadoopJobClient.class);
	private Configuration hadoopConf;
	private final List<RunningJob> runningJobs = new ArrayList<RunningJob>();

	@SuppressWarnings("unused")
	public FlinkHadoopJobClient() throws IOException {
		this(new Configuration());
	}

	public FlinkHadoopJobClient(final JobConf jobConf) throws IOException {
		this(new Configuration(jobConf));
	}

	public FlinkHadoopJobClient(final Configuration hadoopConf)
			throws IOException {
		super(new JobConf(hadoopConf));
		this.hadoopConf = hadoopConf;
	}

	/**
	 * Submits a Hadoop job to Flink (as described by the JobConf) and returns after the job has been completed.
	 *
	 * @param hadoopJobConf the JobConf object to be parsed
	 * @return an instance of a RunningJob, after blocking to finish the job.
	 * @throws IOException
	 */
	public static RunningJob runJob(final JobConf hadoopJobConf) throws IOException {
		final FlinkHadoopJobClient jobClient = new FlinkHadoopJobClient(hadoopJobConf);
		final RunningJob job = jobClient.submitJob(hadoopJobConf);
		job.waitForCompletion();
		return job;
	}

	/**
	 * Submits a job to Flink and returns a RunningJob instance which can be scheduled and monitored
	 * without blocking by default. Use waitForCompletion() to block until the job is finished.
	 *
	 * @param hadoopJobConf the JobConf object to be parsed.
	 * @return an instance of a Running without blocking.
	 * @throws IOException
	 */
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public RunningJob submitJob(final JobConf hadoopJobConf) throws IOException {

		final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		final int mapParallelism = getMapParallelism(hadoopJobConf);
		final int reduceParallelism = getReduceParallelism(hadoopJobConf);

		//setting up the inputFormat for the job
		final org.apache.flink.api.common.io.InputFormat<Tuple2<Writable,Writable>,?> inputFormat = getFlinkInputFormat(hadoopJobConf);
		final DataSource<Tuple2<Writable,Writable>> input = environment.createInput(inputFormat);
		input.setParallelism(mapParallelism);

		final DataSet<Tuple2<Writable, Writable>> result = 
				input.runOperation(new HadoopJobOperation(hadoopJobConf, mapParallelism, reduceParallelism));
		
		final org.apache.hadoop.mapred.OutputFormat<Writable, Writable> hadoopOutputFormat = hadoopJobConf.getOutputFormat();
		final OutputFormat<Tuple2<Writable, Writable>> outputFormat = new HadoopOutputFormat(hadoopOutputFormat, hadoopJobConf);
		if(reduceParallelism == 0) {
			result.output(outputFormat).setParallelism(mapParallelism);
		} else {
			result.output(outputFormat).setParallelism(reduceParallelism);
		}

		return submitJobInternal(environment, hadoopJobConf);
	}

	private RunningJob submitJobInternal(ExecutionEnvironment environment, JobConf conf) throws IOException {

		final Random random = new Random();
		final JobID jobId = new JobID("", random.nextInt(MAX_VALUE));
		Path jobStagingArea = null;
		try {
			jobStagingArea = JobSubmissionFiles.getStagingDir(FlinkHadoopJobClient.this, conf);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		final Path submitJobDir = new Path(jobStagingArea, jobId.toString());

		final JobStatus status = new JobStatus(jobId, 0.0f, 0.0f, JobStatus.RUNNING);
		final String jobFile = new Path(submitJobDir, "job.xml").toString();
		final JobProfile prof = new JobProfile(conf.getUser(), jobId, jobFile, null, conf.getJobName());
		final RunningJob runningJob = new FlinkRunningJob(environment, prof, status, conf);
		this.runningJobs.add(runningJob);
		return runningJob;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private org.apache.flink.api.common.io.InputFormat<Tuple2<Writable,Writable>,?> getFlinkInputFormat(final JobConf jobConf)
			throws IOException{
		final org.apache.hadoop.mapred.InputFormat inputFormat = jobConf.getInputFormat();
		final Class<? extends InputFormat> inputFormatClass = inputFormat.getClass();

		Class keyClass;
		Class valueClass;
		try {
			final TypeInformation keyTypeInfo = TypeExtractor.createTypeInfo(InputFormat.class, inputFormatClass,
					0, null, null);
			keyClass = keyTypeInfo.getTypeClass();

			final TypeInformation valueTypeInfo = TypeExtractor.createTypeInfo(InputFormat.class, inputFormatClass,
					1, null, null);
			valueClass = valueTypeInfo.getTypeClass();
		} catch (InvalidTypesException e) {
			//This happens due to type erasure. As long as there is at least one inputSplit this should work.
			final InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 0);
			if (inputSplits == null) {
				throw new IOException("Cannot extract input types from " + inputFormat + ". No input splits.");
			}

			final InputSplit firstSplit = inputFormat.getSplits(jobConf, 0)[0];
			final Reporter reporter = new HadoopReporter(null);
			keyClass = inputFormat.getRecordReader(firstSplit, jobConf, reporter).createKey().getClass();
			valueClass = inputFormat.getRecordReader(firstSplit, jobConf, reporter).createValue().getClass();
		}

		return new HadoopInputFormat(inputFormat, keyClass, valueClass, jobConf);
	}

	/**
	 * The number of map tasks that can be run in parallel is the minimum of the number of inputSplits and the number
	 * set by the user in the jobconf.
	 * The upper bound for the number of parallel map tasks is the number of Flink task slots.
	 */
	private int getMapParallelism(final JobConf conf) throws IOException {
		final int noOfSplits = conf.getInputFormat().getSplits(conf, 0).length;
		final int hintedMapTasks = conf.getNumMapTasks();
		final int mapTasks = Math.min(noOfSplits, hintedMapTasks);
		return mapTasks > TASK_SLOTS ? TASK_SLOTS : mapTasks;
	}

	/**
	 * The number of reduce tasks that can be run in parallel is set by JobConf.setNumReduceTasks().
	 * The upper bound for the number of parallel reduce tasks is the number of task slots.
	 */
	private int getReduceParallelism(final JobConf conf) {
		final int reduceTasks = conf.getNumReduceTasks();
		if (reduceTasks <= TASK_SLOTS) {
			return reduceTasks;
		} else {
			LOG.warn("The number of reduce tasks (" + reduceTasks + ") exceeds the number of available Flink slots ("
					+ TASK_SLOTS + "). " + TASK_SLOTS + " tasks will be run.");
			return TASK_SLOTS;
		}
	}

	@Override
	public void setConf(Configuration conf) {
		this.hadoopConf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.hadoopConf;
	}


	/**
	 * A Hadoop Job running on Flink.
	 */
	private class FlinkRunningJob implements RunningJob {

		private final ExecutionEnvironment environment;
		private final JobProfile profile;
		private JobStatus status;
		private Counters counters;
		private String failureInfo;
		private JobConf conf;

		public FlinkRunningJob(ExecutionEnvironment environment,
								JobProfile profile,
								JobStatus status,
								JobConf conf) {
			this.environment = environment;
			this.profile = profile;
			this.status = status;
			this.conf = conf;
		}

		@Override
		public JobID getID() {
			return profile.getJobID();
		}

		@Override
		public String getJobID() {
			return profile.getJobID().toString();
		}

		@Override
		public String getJobName() {
			return profile.getJobName();
		}

		@Override
		public String getJobFile() {
			return profile.getJobFile();
		}

		@Override
		public String getTrackingURL() {
			return profile.getURL().toString();
		}

		@Override
		public float mapProgress() throws IOException {
			if (status.getRunState() == JobStatus.SUCCEEDED) {
				return 1.0f;
			} else {
				final String message = "Currently it is not possible to get an accumulator while the job is running.";
				throw new UnsupportedOperationException(message);
			}
		}

		@Override
		public float reduceProgress() throws IOException {
			if (status.getRunState() == JobStatus.SUCCEEDED) {
				return 1.0f;
			} else {
				final String message = "Currently it is not possible to get an accumulator while the job is running.";
				throw new UnsupportedOperationException(message);
			}
		}

		@Override
		public boolean isComplete() throws IOException {
			return (status.getRunState() == JobStatus.SUCCEEDED ||
					status.getRunState() == JobStatus.FAILED ||
					status.getRunState() == JobStatus.KILLED);
		}

		@Override
		public boolean isSuccessful() throws IOException {
			return status.getRunState() == JobStatus.SUCCEEDED;
		}

		/**
		 * Block until the job is completed.
		 *
		 * @throws IOException
		 */
		@Override
		public void waitForCompletion() throws IOException {
			try {
				final JobExecutionResult result = environment.execute(getJobName());
				counters = new FlinkHadoopCounters(result.getAllAccumulatorResults());
			} catch (Exception e) {
				status.setRunState(JobStatus.FAILED);
				LOG.warn("An error has occurred while running the job.", e);
				this.failureInfo = e.toString();
			}
			status.setRunState(JobStatus.SUCCEEDED);
			runningJobs.remove(this);
		}

		@Override
		public int getJobState() throws IOException {
			return status.getRunState();
		}

		@Override
		public JobStatus getJobStatus() throws IOException {
			return status;
		}

		@Override
		public Counters getCounters() throws IOException {
			return counters;
		}

		@Override
		public String getFailureInfo() throws IOException {
			return this.failureInfo;
		}

		@Override
		public String toString() {
			return "Job: " + profile.getJobID() + "\n" +
					"file: " + profile.getJobFile() + "\n" +
					"tracking URL: " + profile.getURL() + "\n";
		}

		@Override
		public void killJob() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public float cleanupProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public float setupProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public synchronized void setJobPriority(final String priority) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public TaskCompletionEvent[] getTaskCompletionEvents(final int startFrom) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killTask(final TaskAttemptID taskId, final boolean shouldFail) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public synchronized void killTask(final String taskId, final boolean shouldFail) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String[] getTaskDiagnostics(final TaskAttemptID taskAttemptID) throws IOException {
			throw new UnsupportedOperationException();
		}

		//Hadoop 2.2 methods.
		@SuppressWarnings("unused")
		public boolean isRetired() throws IOException { throw new UnsupportedOperationException(); }

		@SuppressWarnings("unused")
		public String getHistoryUrl() throws IOException {throw new UnsupportedOperationException(); }

		public Configuration getConfiguration() {
			return this.conf;
		}
	}

	@Override
	public void init(final JobConf conf) throws IOException {
		try {
			FieldUtils.writeField(this, "jobSubmitClient", new FlinkHadoopJobSubmitClient(conf), true);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("System error.");
		}
		try {
			FieldUtils.writeField(this, "ugi", UserGroupInformation.getCurrentUser(), true);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("System error.");
		}

	}

	@Override
	public RunningJob getJob(JobID jobid) throws IOException {
		for (RunningJob job : this.runningJobs) {
			if (job.getID().equals(jobid)) {
				return job;
			}
		}
		return null;
	}

	@Override
	public int getDefaultMaps() throws IOException {
		return TASK_SLOTS;
	}

	@Override
	public int getDefaultReduces() throws IOException {
		return TASK_SLOTS;
	}

	@Override
	public RunningJob submitJobInternal(JobConf job) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TaskReport[] getMapTaskReports(JobID jobId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void displayTasks(JobID jobId, String type, String state) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ClusterStatus getClusterStatus() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean monitorAndPrintJob(JobConf conf, RunningJob job) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int run(java.lang.String[] argv) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobStatus[] getJobsFromQueue(String queueName) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobQueueInfo getQueueInfo(String queueName) throws IOException {
		throw new UnsupportedOperationException();
	}
}
