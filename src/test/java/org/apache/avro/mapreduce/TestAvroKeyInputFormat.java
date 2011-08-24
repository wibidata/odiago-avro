// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

public class TestAvroKeyInputFormat {
  /**
   * Verifies that a non-null record reader can be created, and the key/value types are
   * as expected.
   */
  @Test
  public void testCreateRecordReader() throws IOException, InterruptedException {
    // Set up the job configuration.
    Job job = new Job();
    AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
    Configuration conf = job.getConfiguration();

    FileSplit inputSplit = createMock(FileSplit.class);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();

    replay(inputSplit);
    replay(context);

    AvroKeyInputFormat inputFormat = new AvroKeyInputFormat();
    RecordReader<AvroKey<Object>, NullWritable> recordReader = inputFormat.createRecordReader(
        inputSplit, context);
    assertNotNull(inputFormat);
    recordReader.close();

    verify(inputSplit);
    verify(context);
  }
}
