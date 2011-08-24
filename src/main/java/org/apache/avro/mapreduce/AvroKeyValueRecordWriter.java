// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes key/value pairs to an Avro container file.
 *
 * <p>Each entry in the Avro container file will be a generic record with two fields,
 * named 'key' and 'value'.  The input types may be basic Writable objects like Text or
 * IntWritable, or they may be AvroWrapper subclasses (AvroKey or AvroValue).  Writable
 * objects will be converted to their corresponding Avro types when written to the generic
 * record key/value pair.</p>
 *
 * @param <K> The type of key to write.
 * @param <V> The type of value to write.
 */
public class AvroKeyValueRecordWriter<K, V> extends RecordWriter<K, V> {
  /** A writer for the Avro container file. */
  private final DataFileWriter<GenericRecord> mAvroFileWriter;

  /** The writer schema for the generic record entries of the Avro container file. */
  private final Schema mKeyValuePairSchema;

  /** A reusable Avro generic record for writing key/value pairs to the file. */
  private final AvroKeyValue<Object, Object> mOutputRecord;

  /** A helper object that converts the input key to an Avro datum. */
  private final AvroDatumConverter<K, ?> mKeyConverter;

  /** A helper object that converts the input value to an Avro datum. */
  private final AvroDatumConverter<V, ?> mValueConverter;

  public AvroKeyValueRecordWriter(AvroDatumConverter<K, ?> keyConverter,
      AvroDatumConverter<V, ?> valueConverter, CodecFactory compressionCodec,
      OutputStream outputStream) throws IOException {
    // Create the generic record schema for the key/value pair.
    mKeyValuePairSchema = AvroKeyValue.getSchema(
        keyConverter.getWriterSchema(), valueConverter.getWriterSchema());

    // Create an Avro container file and a writer to it.
    mAvroFileWriter = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(mKeyValuePairSchema));
    mAvroFileWriter.setCodec(compressionCodec);
    mAvroFileWriter.create(mKeyValuePairSchema, outputStream);

    // Keep a reference to the converters.
    mKeyConverter = keyConverter;
    mValueConverter = valueConverter;

    // Create a reusable output record.
    mOutputRecord = new AvroKeyValue(new GenericData.Record(mKeyValuePairSchema));
  }

  /**
   * Gets the writer schema for the key/value pair generic record.
   *
   * @return The writer schema used for entries of the Avro container file.
   */
  public Schema getWriterSchema() {
    return mKeyValuePairSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void write(K key, V value) throws IOException {
    mOutputRecord.setKey(mKeyConverter.convert(key));
    mOutputRecord.setValue(mValueConverter.convert(value));
    mAvroFileWriter.append(mOutputRecord.get());
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    mAvroFileWriter.close();
  }
}
