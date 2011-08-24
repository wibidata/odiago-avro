// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FileOutputFormat for writing Avro container files of key/value pairs.
 *
 * <p>Since Avro container files can only contain records (not key/value pairs), this
 * output format puts the key and value into an Avro generic record with two fields, named
 * 'key' and 'value'.</p>
 *
 * <p>The keys and values given to this output format may be Avro objects wrapped in
 * <code>AvroKey</code> or <code>AvroValue</code> objects.  The basic Writable types are
 * also supported (e.g., IntWritable, Text); they will be converted to their corresponding
 * Avro types.</p>
 *
 * @param <K> The type of key. If an Avro type, it must be wrapped in an <code>AvroKey</code>.
 * @param <V> The type of value. If an Avro type, it must be wrapped in an <code>AvroValue</code>.
 */
public class AvroKeyValueOutputFormat<K, V> extends AvroOutputFormatBase<K, V> {
  /** {@inheritDoc} */
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
    AvroDatumConverterFactory converterFactory = new AvroDatumConverterFactory(
        context.getConfiguration());

    AvroDatumConverter<K, ?> keyConverter = converterFactory.create(
        (Class<K>) context.getOutputKeyClass());
    AvroDatumConverter<V, ?> valueConverter = converterFactory.create(
        (Class<V>) context.getOutputValueClass());

    return new AvroKeyValueRecordWriter<K, V>(keyConverter, valueConverter,
        getCompressionCodec(context), getAvroFileOutputStream(context));
  }
}
