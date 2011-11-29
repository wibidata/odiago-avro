// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Serializes AvroWrapper objects within Hadoop.
 *
 * <p>Keys and values containing Avro types are more efficiently serialized outside of the
 * WritableSerialization model, so they are wrapped in {@link
 * org.apache.avro.mapred.AvroWrapper} objects and serialization is handled by this
 * class.</p>
 *
 * <p>MapReduce jobs that use AvroWrapper objects as keys or values need to be configured
 * with {@link org.apache.avro.mapreduce.AvroSerialization}.  Use {@link
 * org.apache.avro.mapreduce.AvroJob} to help with Job configuration.</p>
 *
 * @param <T> The Java type of the Avro data.
 */
public class AvroSerializer<T> implements Serializer<AvroWrapper<T>> {
  /**
   * The block size for the Avro encoder.
   *
   * This number was copied from the AvroSerialization of org.apache.avro.mapred in Avro 1.5.1.
   *
   * TODO(gwu): Do some benchmarking with different numbers here to see if it is important.
   */
  private static final int AVRO_ENCODER_BLOCK_SIZE_BYTES = 512;

  /** An factory for creating Avro datum encoders. */
  private static EncoderFactory mEncoderFactory
      = new EncoderFactory().configureBlockSize(AVRO_ENCODER_BLOCK_SIZE_BYTES);

  /** The writer schema for the data to serialize. */
  private final Schema mWriterSchema;

  /** The Avro datum writer for serializing. */
  private final DatumWriter<T> mAvroDatumWriter;

  /** The Avro encoder for serializing. */
  private BinaryEncoder mAvroEncoder;

  /** The output stream for serializing. */
  private OutputStream mOutputStream;

  /**
   * Constructor.
   *
   * @param writerSchema The writer schema for the Avro data being serialized.
   */
  public AvroSerializer(Schema writerSchema) {
    if (null == writerSchema) {
      throw new IllegalArgumentException("Writer schema may not be null");
    }
    mWriterSchema = writerSchema;
    mAvroDatumWriter = new SpecificDatumWriter<T>(writerSchema);
  }

  /**
   * Gets the writer schema being used for serialization.
   *
   * @return The writer schema.
   */
  public Schema getWriterSchema() {
    return mWriterSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void open(OutputStream outputStream) throws IOException {
    mOutputStream = outputStream;
    mAvroEncoder = mEncoderFactory.binaryEncoder(outputStream, mAvroEncoder);
  }

  /** {@inheritDoc} */
  @Override
  public void serialize(AvroWrapper<T> avroWrapper) throws IOException {
    mAvroDatumWriter.write(avroWrapper.datum(), mAvroEncoder);
    // This would be a lot faster if the Serializer interface had a flush() method and the
    // Hadoop framework called it when needed.  For now, we'll have to flush on every record.
    mAvroEncoder.flush();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mOutputStream.close();
  }
}
