// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * The {@link org.apache.hadoop.io.serializer.Serialization} used by jobs configured with
 * {@link org.apache.avro.mapreduce.AvroJob}.
 *
 * @param <T> The Java type of the Avro data to serialize.
 */
public class AvroSerialization<T> extends Configured implements Serialization<AvroWrapper<T>> {
  /** {@inheritDoc} */
  @Override
  public boolean accept(Class<?> c) {
    return AvroKey.class.isAssignableFrom(c) || AvroValue.class.isAssignableFrom(c);
  }

  /**
   * Gets an object capable of deserializing the output from a Mapper.
   *
   * @param c The class to get a deserializer for.
   * @return A deserializer for objects of class <code>c</code>.
   */
  @Override
  public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
    if (AvroKey.class.isAssignableFrom(c)) {
      return new AvroKeyDeserializer(AvroJob.getMapOutputKeySchema(getConf()));
    } else if (AvroValue.class.isAssignableFrom(c)) {
      return new AvroValueDeserializer(AvroJob.getMapOutputValueSchema(getConf()));
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
  }

  /**
   * Gets an object capable of serializing output from a Mapper.
   *
   * <p>This may be for Map output
   */
  public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
    Schema schema;
    if (AvroKey.class.isAssignableFrom(c)) {
      schema = AvroJob.getMapOutputKeySchema(getConf());
    } else if (AvroValue.class.isAssignableFrom(c)) {
      schema = AvroJob.getMapOutputValueSchema(getConf());
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
    return new AvroSerializer<T>(schema);
  }
}
