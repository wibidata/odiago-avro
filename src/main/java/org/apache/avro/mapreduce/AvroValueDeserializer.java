// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;

/**
 * Deserializes AvroValue objects within Hadoop.
 *
 * @param <D> The java type of the avro data to deserialize.
 *
 * @see org.apache.avro.mapreduce.AvroDeserializer
 */
public class AvroValueDeserializer<D> extends AvroDeserializer<AvroValue<D>, D> {
  /**
   * Constructor.
   *
   * @param readerSchema The Avro reader schema for the data to deserialize.
   */
  public AvroValueDeserializer(Schema readerSchema) {
    super(readerSchema);
  }

  /**
   * Creates a new empty <code>AvroValue</code> instance.
   *
   * @return a new empty AvroValue.
   */
  @Override
  protected AvroValue<D> createAvroWrapper() {
    return new AvroValue<D>(null);
  }
}
