// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.io;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestAvroSerialization {
  @Test
  public void testAccept() {
    AvroSerialization<CharSequence> serialization = new AvroSerialization<CharSequence>();

    assertTrue(serialization.accept(AvroKey.class));
    assertTrue(serialization.accept(AvroValue.class));
    assertFalse(serialization.accept(AvroWrapper.class));
    assertFalse(serialization.accept(String.class));
  }

  @Test
  public void testGetSerializerForKey() throws IOException {
    // Set the writer schema in the job configuration.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    Job job = new Job();
    AvroJob.setMapOutputKeySchema(job, writerSchema);

    // Get a serializer from the configuration.
    AvroSerialization serialization
        = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Serializer<AvroWrapper> serializer = serialization.getSerializer(AvroKey.class);
    assertTrue(serializer instanceof AvroSerializer);
    AvroSerializer avroSerializer = (AvroSerializer) serializer;

    // Check that the writer schema is set correctly on the serializer.
    assertEquals(writerSchema, avroSerializer.getWriterSchema());
  }

  @Test
  public void testGetSerializerForValue() throws IOException {
    // Set the writer schema in the job configuration.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    Job job = new Job();
    AvroJob.setMapOutputValueSchema(job, writerSchema);

    // Get a serializer from the configuration.
    AvroSerialization serialization
        = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Serializer<AvroWrapper> serializer = serialization.getSerializer(AvroValue.class);
    assertTrue(serializer instanceof AvroSerializer);
    AvroSerializer avroSerializer = (AvroSerializer) serializer;

    // Check that the writer schema is set correctly on the serializer.
    assertEquals(writerSchema, avroSerializer.getWriterSchema());
  }

  @Test
  public void testGetDeserializerForKey() throws IOException {
    // Set the reader schema in the job configuration.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    Job job = new Job();
    AvroJob.setMapOutputKeySchema(job, readerSchema);

    // Get a deserializer from the configuration.
    AvroSerialization serialization
        = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Deserializer<AvroWrapper> deserializer = serialization.getDeserializer(AvroKey.class);
    assertTrue(deserializer instanceof AvroKeyDeserializer);
    AvroKeyDeserializer avroDeserializer = (AvroKeyDeserializer) deserializer;

    // Check that the reader schema is set correctly on the deserializer.
    assertEquals(readerSchema, avroDeserializer.getReaderSchema());
  }

  @Test
  public void testGetDeserializerForValue() throws IOException {
    // Set the reader schema in the job configuration.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    Job job = new Job();
    AvroJob.setMapOutputValueSchema(job, readerSchema);

    // Get a deserializer from the configuration.
    AvroSerialization serialization
        = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Deserializer<AvroWrapper> deserializer = serialization.getDeserializer(AvroValue.class);
    assertTrue(deserializer instanceof AvroValueDeserializer);
    AvroValueDeserializer avroDeserializer = (AvroValueDeserializer) deserializer;

    // Check that the reader schema is set correctly on the deserializer.
    assertEquals(readerSchema, avroDeserializer.getReaderSchema());
  }
}
