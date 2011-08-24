// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class TestAvroDatumConverterFactory {
  private Job mJob;
  private AvroDatumConverterFactory mFactory;

  @Before
  public void setup() throws IOException {
    mJob = new Job();
    mFactory = new AvroDatumConverterFactory(mJob.getConfiguration());
  }

  @Test
  public void testConvertAvroKey() throws IOException {
    AvroJob.setOutputKeySchema(mJob, Schema.create(Schema.Type.STRING));

    AvroKey<CharSequence> avroKey = new AvroKey<CharSequence>("foo");
    AvroDatumConverter<AvroKey<CharSequence>, ?> converter = mFactory.create(
        (Class<AvroKey<CharSequence>>) avroKey.getClass());
    assertEquals("foo", converter.convert(avroKey).toString());
  }

  @Test
  public void testConvertAvroValue() throws IOException {
    AvroJob.setOutputValueSchema(mJob, Schema.create(Schema.Type.INT));

    AvroValue<Integer> avroValue = new AvroValue<Integer>(42);
    AvroDatumConverter<AvroValue<Integer>, Integer> converter = mFactory.create(
        (Class<AvroValue<Integer>>) avroValue.getClass());
    assertEquals(42, converter.convert(avroValue).intValue());
  }

  @Test
  public void testConvertBooleanWritable() {
    AvroDatumConverter<BooleanWritable, Boolean> converter
        = mFactory.create(BooleanWritable.class);
    assertEquals(true, converter.convert(new BooleanWritable(true)).booleanValue());
  }

  @Test
  public void testConvertBytesWritable() {
    AvroDatumConverter<BytesWritable, ByteBuffer> converter = mFactory.create(BytesWritable.class);
    ByteBuffer bytes = converter.convert(new BytesWritable(new byte[] { 1, 2, 3 }));
    assertEquals(1, bytes.get(0));
    assertEquals(2, bytes.get(1));
    assertEquals(3, bytes.get(2));
  }

  @Test
  public void testConvertByteWritable() {
    AvroDatumConverter<ByteWritable, GenericFixed> converter = mFactory.create(ByteWritable.class);
    assertEquals(42, converter.convert(new ByteWritable((byte) 42)).bytes()[0]);
  }

  @Test
  public void testConvertDoubleWritable() {
    AvroDatumConverter<DoubleWritable, Double> converter = mFactory.create(DoubleWritable.class);
    assertEquals(2.0, converter.convert(new DoubleWritable(2.0)).doubleValue(), 0.00001);
  }

  @Test
  public void testConvertFloatWritable() {
    AvroDatumConverter<FloatWritable, Float> converter = mFactory.create(FloatWritable.class);
    assertEquals(2.2f, converter.convert(new FloatWritable(2.2f)).floatValue(), 0.00001);
  }

  @Test
  public void testConvertIntWritable() {
    AvroDatumConverter<IntWritable, Integer> converter = mFactory.create(IntWritable.class);
    assertEquals(2, converter.convert(new IntWritable(2)).intValue());
  }

  @Test
  public void testConvertLongWritable() {
    AvroDatumConverter<LongWritable, Long> converter = mFactory.create(LongWritable.class);
    assertEquals(123L, converter.convert(new LongWritable(123L)).longValue());
  }

  @Test
  public void testConvertNullWritable() {
    AvroDatumConverter<NullWritable, Object> converter = mFactory.create(NullWritable.class);
    assertNull(converter.convert(NullWritable.get()));
  }

  @Test
  public void testConvertText() {
    AvroDatumConverter<Text, CharSequence> converter = mFactory.create(Text.class);
    assertEquals("foo", converter.convert(new Text("foo")).toString());
  }
}
