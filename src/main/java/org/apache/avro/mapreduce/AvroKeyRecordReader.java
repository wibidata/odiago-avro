// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads records from an input split representing a chunk of an Avro container file.
 *
 * @param <T> The (java) type of data in Avro container file.
 */
public class AvroKeyRecordReader<T> extends AvroRecordReaderBase<AvroKey<T>, NullWritable, T> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyRecordReader.class);

  /** A reusable object to hold records of the Avro container file. */
  private final AvroKey<T> mCurrentRecord;

  /**
   * Constructor.
   *
   * @param readerSchema The reader schema to use for the records in the Avro container file.
   */
  public AvroKeyRecordReader(Schema readerSchema) {
    super(readerSchema);
    mCurrentRecord = new AvroKey<T>(null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = super.nextKeyValue();
    mCurrentRecord.datum(getCurrentRecord());
    return hasNext;
  }

  /** {@inheritDoc} */
  @Override
  public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
    return mCurrentRecord;
  }

  /** {@inheritDoc} */
  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }
}
