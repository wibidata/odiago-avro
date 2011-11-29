// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;

/**
 * Deserializes AvroKey objects within Hadoop.
 *
 * @param <D> The java type of the avro data to deserialize.
 *
 * @see org.apache.avro.io.AvroDeserializer
 */
public class AvroKeyDeserializer<D> extends AvroDeserializer<AvroWrapper<D>, D> {
  /**
   * Constructor.
   *
   * @param writerSchema The Avro writer schema for the data to deserialize.
   * @param readerSchema The Avro reader schema for the data to deserialize.
   */
  public AvroKeyDeserializer(Schema writerSchema, Schema readerSchema) {
    super(writerSchema, readerSchema);
  }

  /**
   * Creates a new empty <code>AvroKey</code> instance.
   *
   * @return a new empty AvroKey.
   */
  @Override
  protected AvroWrapper<D> createAvroWrapper() {
    return new AvroKey<D>(null);
  }
}
