// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroKeyOutputFormat {
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Test
  public void testWithNullCodec() throws IOException {
    Configuration conf = new Configuration();
    testGetRecordWriter(conf, CodecFactory.nullCodec());
  }

  @Test
  public void testWithDeflateCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.output.compress", true);
    conf.setInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY, 3);
    testGetRecordWriter(conf, CodecFactory.deflateCodec(3));
  }

  /**
   * Tests that the record writer is contructed and returned correclty from the output format.
   */
  private void testGetRecordWriter(Configuration conf, CodecFactory expectedCodec)
      throws IOException {
    // Configure a mock task attempt context.
    Job job = new Job(conf);
    job.getConfiguration().set("mapred.output.dir", mTempDir.getRoot().getPath());
    Schema writerSchema = Schema.create(Schema.Type.INT);
    AvroJob.setOutputKeySchema(job, writerSchema);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration())
        .andReturn(job.getConfiguration()).anyTimes();
    expect(context.getTaskAttemptID())
        .andReturn(new TaskAttemptID("id", 1, true, 1, 1))
        .anyTimes();

    // Create a mock record writer.
    RecordWriter<AvroKey<Integer>, NullWritable> expectedRecordWriter
        = createMock(RecordWriter.class);
    AvroKeyOutputFormat.RecordWriterFactory recordWriterFactory
        = createMock(AvroKeyOutputFormat.RecordWriterFactory.class);

    // Expect the record writer factory to be called with appropriate parameters.
    Capture<CodecFactory> capturedCodecFactory = new Capture<CodecFactory>();
    expect(recordWriterFactory.create(eq(writerSchema),
        capture(capturedCodecFactory),  // Capture for comparison later.
        anyObject(OutputStream.class))).andReturn(expectedRecordWriter);

    replay(context);
    replay(expectedRecordWriter);
    replay(recordWriterFactory);

    AvroKeyOutputFormat<Integer> outputFormat
        = new AvroKeyOutputFormat<Integer>(recordWriterFactory);
    RecordWriter<AvroKey<Integer>, NullWritable> recordWriter
        = outputFormat.getRecordWriter(context);
    // Make sure the expected codec was used.
    assertTrue(capturedCodecFactory.hasCaptured());
    assertEquals(expectedCodec.toString(), capturedCodecFactory.getValue().toString());

    verify(context);
    verify(expectedRecordWriter);
    verify(recordWriterFactory);

    assertNotNull(recordWriter);
    assertTrue(expectedRecordWriter == recordWriter);
  }
}
