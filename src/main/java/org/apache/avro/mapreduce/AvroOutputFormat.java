package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;

/**
 * FileOutputFormat for writing avro container files.
 */
public class AvroOutputFormat<T> extends FileOutputFormat<AvroKey<T>, NullWritable> {

  @Override
  public RecordWriter<AvroKey<T>, NullWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    Schema schema = AvroJob.getOutputSchema(context.getConfiguration());
    if (schema == null) {
      throw new RuntimeException("AvroOutputFormat requires an output schema.");
    }
	
    final DataFileWriter<T> writer =
        new DataFileWriter<T>(new SpecificDatumWriter<T>());
	
    if (FileOutputFormat.getCompressOutput(context)) {
      int level = context.getConfiguration().getInt(
          org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
          org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
    }

    Path path = getDefaultWorkFile(context, org.apache.avro.mapred.AvroOutputFormat.EXT);
    writer.create(schema, path.getFileSystem(context.getConfiguration()).create(path));
	
    return new RecordWriter<AvroKey<T>, NullWritable>() {
      public void write(AvroKey<T> record, NullWritable ignore) throws IOException {
        writer.append(record.datum());
      }
      public void close(TaskAttemptContext context) throws IOException {
        writer.close();
      }
    };
  }
}
