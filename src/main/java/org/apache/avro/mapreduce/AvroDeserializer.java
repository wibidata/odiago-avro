// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * Deserializes AvroWrapper objects within Hadoop.
 *
 * <p>Keys and values containing Avro tyeps are more efficiently serialized outside of the
 * WritableSerialization model, so they are wrapper in {@link
 * org.apache.avro.mapred.AvroWrapper} objects and deserialization is handled by this
 * class.</p>
 *
 * <p>MapReduce jobs that use AvroWrapper objects as keys or values need to be configured
 * with {@link org.apache.avro.mapreduce.AvroSerialization}.  Use {@link
 * org.apache.avro.mapreduce.AvroJob} to help with Job configuration.</p>
 *
 * @param <T> The type of Avro wrapper.
 * @param <D> The Java type of the Avro data being wrapped.
 */
public abstract class AvroDeserializer<T extends AvroWrapper<D>, D> implements Deserializer<T> {
  /** The Avro reader schema for deserializing. */
  private final Schema mReaderSchema;

  /** The Avro datum reader for deserializing. */
  private final DatumReader<D> mAvroDatumReader;

  /** An Avro binary decoder for deserializing. */
  private BinaryDecoder mAvroDecoder;

  /**
   * Constructor.
   *
   * @param readerSchema The Avro reader schema for the data to deserialize.
   */
  protected AvroDeserializer(Schema readerSchema) {
    mReaderSchema = readerSchema;
    mAvroDatumReader = new SpecificDatumReader<D>(readerSchema);
  }

  /**
   * Gets the reader schema used for deserializing.
   *
   * @return The reader schema.
   */
  public Schema getReaderSchema() {
    return mReaderSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void open(InputStream inputStream) throws IOException {
    mAvroDecoder = DecoderFactory.get().directBinaryDecoder(inputStream, mAvroDecoder);
  }

  /** {@inheritDoc} */
  @Override
  public T deserialize(T avroWrapperToReuse) throws IOException {
    // Create a new Avro wrapper if there isn't one to reuse.
    if (null == avroWrapperToReuse) {
      avroWrapperToReuse = createAvroWrapper();
    }

    // Deserialize the Avro datum from the input stream.
    avroWrapperToReuse.datum(mAvroDatumReader.read(avroWrapperToReuse.datum(), mAvroDecoder));
    return avroWrapperToReuse;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mAvroDecoder.inputStream().close();
  }

  /**
   * Creates a new empty <code>T</code> (extends AvroWrapper) instance.
   *
   * @return A new empty <code>T</code> instance.
   */
  protected abstract T createAvroWrapper();
}
