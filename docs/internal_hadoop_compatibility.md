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

MAP

REDUCE (COMBINE)

Partition

Group value

Compare key

Sort




- JobClient...


Counters


wrappers for outputcollector and reporter