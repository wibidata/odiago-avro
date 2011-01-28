// (c) Copyright 2010 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Reads records from a file input split of Avro messages.
 */
public class AvroRecordReader<T> extends RecordReader<AvroKey<T>, NullWritable> {

  /** An Avro file reader that knows how to seek to record boundries. */
  private DataFileReader<T> mReader;
  /** Start of the input split in bytes offset. */
  private long mStart;
  /** End of the input split in bytes offset. */
  private long mEnd;
  /** Current record from the input split. */
  private AvroKey<T> mCurrentRecord;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) 
    throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    // Create a DataFileReader that knows how to read records out of the input split.
    SeekableInput fileInput = new FsInput(fileSplit.getPath(), context.getConfiguration());
    // We have to get the schema to read the records, which we assume was set in
    // the job's configuration.
    Schema schema = AvroJob.getInputSchema(context.getConfiguration());
    mReader = new DataFileReader<T>(fileInput, new SpecificDatumReader<T>(schema));

    // Initialize the start and end offsets into the file that we're going to read.
    // The reader here syncs to the first record after the start of the input split.
    mReader.sync(fileSplit.getStart());
    mStart = mReader.previousSync();
    mEnd = fileSplit.getStart() + fileSplit.getLength();

    mCurrentRecord = new AvroKey<T>(null);
  }

  @Override
  public void close() throws IOException {
    mReader.close();
  }

  @Override
  public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
    return mCurrentRecord;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (mEnd == mStart) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (float) (mReader.previousSync() - mStart) / (mEnd - mStart));
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (mReader.hasNext() && !mReader.pastSync(mEnd)) {
      mCurrentRecord.datum(mReader.next(mCurrentRecord.datum()));
      return true;
    }
    return false;
  }
}
