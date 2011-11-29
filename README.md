odiago-avro
===========

Extensions to the [Apache Avro](http://avro.apache.org) project for
working with [Apache Hadoop](http://hadoop.apache.org).  Many of the
components in this project may be moved into the Avro project
eventually, though this repository serves as a good home for the
application-level features not part of the Avro core.


### Features

* `org.apache.hadoop.io.AvroSequenceFile` - A wrapper around a Hadoop
  SequenceFile that supports reading and writing Avro data.
* `org.apache.avro.mapreduce` - A package for using Avro as
  input/output records from a Hadoop MapReduce job using the new API.
* `org.apache.avro.io.AvroDatumConverter` - Convert between Hadoop IO
  types (`Text`, `IntWritable`) to Avro-mapped Java types
  (`CharSequence`, `Integer`).
* `org.apache.avro.io.AvroKeyValue` - Work with key-value pairs that
  can be serialized and deserialized from Avro conatiner files.
* `org.apache.avro.util.AvroCharSequenceComparator` - Work with any
  Java CharSequence implementation of the Avro `"string"` type.
* `org.apache.avro.file.SortedKeyValueFile` - Store sorted key-value
  pairs in an indexed Avro container file (like a Hadoop MapFile).


### How to build

To build `target/odiago-avro-${project.version}.jar` run:

    mvn package


### Documentation

* [Java API (Javadoc)](http://wibidata.github.com/odiago-avro/1.0.5/apidocs/)
