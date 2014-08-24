---
title: "Hadoop Compatibility"
---

<section id="top">
Introduction
------------

It is possible to run Apache Hadoop jobs on Apache Flink without modifying (? ClassLoader) the code of the Hadoop job driver. Currently, the ```mapred``` API is the one in which programming interfaces are being supported. However, adding support to the ```mapreduce``` API is in the short-term plans.

Hadoop's ```InputFormat``` and ```OutputFormat``` are supported for both APIs.


Most Hadoop programming interfaces are supported. See [supported interfaces](#supported) for details.


<section id="example">
Example
-------

Consider the following Hadoop Job:

```java
public class FullWordCount {

	public static void main(final String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FullWordCount <input path> <result path>");
			return;
		}
		final String inputPath = arg[0];
		final String outputPath = arg[1];

		final JobConf conf = new JobConf();

		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(TokenCountMapper.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setCombinerClass((LongSumReducer.class));

		conf.set("mapred.textoutputformat.separator", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		FlinkHadoopJobClient.runJob(conf);
	}
}
```

The only difference in the above job driver and a regular Hadoop driver is the fact that a subclass of the ```JobClient``` called ```FlinkHadoopJobClient``` is being used. This custom ```FlinkHadoopJobClient``` is the entry point to our application.

Hadoop Driver As Is
-------------------

It will soon be possible to run Hadoop jobs without the need to substitute the ```JobClient``` with a ```FlinkHadoopJobClient```.


<section id="supported">
Supported interfaces
--------------------

All ```mapred``` Hadoop programming interfaces are supported. However, some limitations still apply to some of them.

Programming Interface | Supported ?
Mapper | Fully
Reducer | Fully
Combiner | Not possible to set a grouping comparator (2.x API feature)
Key-sorting comparator | Fully
Value-grouping comparator | Reducer only
Partitioner | Fully
Counters | User-defined counters only and after the successful completion of a jbo 
DistributedCache | Fully (deprecated, though)


<section id="parallelism">

Number of Map and Reduce Tasks
------------------------------

There are several differences between Flink and Hadoop in terms of parallelism. The degree of parallelism of n for a Flink operator practically means that n tasks will be executed in parallel (with a pre-defined upper bound with the value of ```ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS```).

For Apache Hadoop, it is possible to define a certain number of tasks but execute as much as the number of task slots everytime.

Therefore, the number of tasks for map or reduce cannot exceed the number of Flink max task slots.

*Warning*
The user should be wary that a different number of reduce tasks may lead into a different result. A relevant warning is being raised.


<section if ="operation">
HadoopJobOperation
------------------

It is possible to define a custom unary operation for Hadoop interfaces. For a given conf and a map and reduce parallelism it should be:

```java
final DataSet<Tuple2<Writable, Writable>> result = input.runOperation(new HadoopJobOperation(hadoopJobConf, mapParallelism, reduceParallelism));
```
Any Flink InputFormat or OutputFormat can be used for this operation.