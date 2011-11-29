// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.io;

import org.apache.avro.Schema;

/**
 * Converts a Java object into an Avro datum.
 *
 * @param <INPUT> The type of the input Java object to convert.
 * @param <OUTPUT> The type of the Avro datum to convert to.
 */
public abstract class AvroDatumConverter<INPUT, OUTPUT> {
  public abstract OUTPUT convert(INPUT input);

  /**
   * Gets the writer schema that should be used to serialize the output Avro datum.
   *
   * @return The writer schema for the output Avro datum.
   */
  public abstract Schema getWriterSchema();
}
