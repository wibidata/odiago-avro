// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;

/**
 * Deserializes AvroKey objects within Hadoop.
 *
 * @param <D> The java type of the avro data to deserialize.
 *
 * @see org.apache.avro.mapreduce.AvroDeserializer
 */
public class AvroKeyDeserializer<D> extends AvroDeserializer<AvroKey<D>, D> {
  /**
   * Constructor.
   *
   * @param readerSchema The Avro reader schema for the data to deserialize.
   */
  public AvroKeyDeserializer(Schema readerSchema) {
    super(readerSchema);
  }

  /**
   * Creates a new empty <code>AvroKey</code> instance.
   *
   * @return a new empty AvroKey.
   */
  @Override
  protected AvroKey<D> createAvroWrapper() {
    return new AvroKey<D>(null);
  }
}
