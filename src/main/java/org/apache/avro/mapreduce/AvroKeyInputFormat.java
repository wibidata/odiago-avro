// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A MapReduce InputFormat that can handle Avro container files.
 *
 * <p>Keys are AvroKey wrapper objects that contain the Avro data.  Since Avro container
 * files store only records (not key/value pairs), the value from this InputFormat is a
 * NullWritable.</p>
 */
public class AvroKeyInputFormat<T> extends FileInputFormat<AvroKey<T>, NullWritable> {
  /** {@inheritDoc} */
  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    Schema readerSchema = AvroJob.getInputSchema(context.getConfiguration());
    if (null == readerSchema) {
      throw new IOException("Reader schema was not set. Use AvroJob.setInputSchema()");
    }
    return new AvroKeyRecordReader<T>(readerSchema);
  }
}
