---
title: "Hadoop Compatibility Internals"
---

<section id="top">
Introduction
------------

Flink's compatibility layer of Apache Hadoop is generally supported by wrapping each programming interface of Hadoop by the Flink equivalent function and operator(s) and by parsing the user's ```JobConf``` by a subclass of a ```JobClient``` called ```FlinkHadoopJobClient```.


<section id="interfaces">

Interfaces
----------

*Basic*

Basically, each Hadoop-related functionality is being wrapped by Flink. Then, the ```FlinkHadoopJobClient``` runs a job by using Flink's functions and operators. It is necessary to provide custom serialization functions for the wrapped Hadoop fields due to Hadoop's different serialization mechanism. The simplest example is the wrapped Hadoop Map which is wrapped by the following ```HadoopMapFunction```. To understand the idea, here is the flatMap function that wraps a mapper:


```java
	/**
	 * Wrap a hadoop map() function call and use a Flink collector to collect the result values.
	 *
	 * @param value The input value.
	 * @param out   The collector for emitting result values.
	 * @throws Exception
	 */
	@Override
	public void flatMap(final Tuple2<KEYIN, VALUEIN> value, final Collector<Tuple2<KEYOUT, VALUEOUT>> out)
			throws Exception {
		outputCollector.set(out);
		mapper.map(value.f0, value.f1, outputCollector, reporter);
	}
```


The InputFormats and OutputFormatas are being transformed in the same manner.


*Reducer/Combiner*

The reducer wrapper function is a bit more complex. Since, all combiners are basically reducers the ```HadoopReduceFunction``` (which is a ```GroupReduceFunction```) supports both reduce and combine. Moreover, this ```GroupReduceFunction``` encapsulates the logic behind the custom partitioning and comparing values and keys along with a custom ```HadoopReduceOperator```. The main reason for that, is the fact that we need to support partitioning and grouping values before reducing (on which values to call reduce?) on a different *key*, which is not a standard case for Apache Flink.

*Partitioner, KeyComparator, ValueGroupComparator*

These interfaces are trnaslated to ```KeySelector``` extensions. 

* Collection of key-value pairs and Counters*

The collection of output key-value pairs and the reporting of status are the two ways that a task (map or reduce) achieves interaction with the underlying framework. By default, this framework is of course Hadoop, but this can be changed by providing custom implementations of these interfaces. Therefore, custom implementations of an ```OutputCollector``` and a ```Reporter``` are provided to do tasks on Flink.

The ```HadoopOutputCollector``` wraps a Flink ```Collector``` so that the output reaches Flink and not Hadoop and the ```HadoopReporter``` updates an ```Accumulator``` everytime a ```Counter``` is updated.

Note: Only user-defined counters are currently supported and it is possible to get their result after the successful execution of the job.


JobClient
---------

The custom ```FlinkHadoopJobClient``` is the starting point for every job. Each job submission to the framework results into a ```RunningJob```. The API should be as identical as possible.

```java
JobClient client = new FlinkHadoopJobClient();
RunningJob runningJob = client.submitJob(jobConf);   //the JobConf of the job
runningJob.waitForCompletion();
Counters counters = runningJob.getCounters();		
System.out.println(counters);			     // prints the counters of the job.
```