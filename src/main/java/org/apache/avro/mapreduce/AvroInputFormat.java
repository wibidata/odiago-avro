package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A MapReduce InputFormat that can handle Avro container files.
 */
public class AvroInputFormat<T> extends FileInputFormat<AvroKey<T>, NullWritable> {

  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new AvroRecordReader<T>();
  }
}
