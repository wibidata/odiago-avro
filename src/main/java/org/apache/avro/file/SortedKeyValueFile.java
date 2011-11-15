// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SortedKeyValueFile is an indexed Avro container file of KeyValue records sorted by key.
 *
 * <p>The SortedKeyValueFile is a directory with two files, named 'data' and 'index'. The
 * 'data' file is an ordinary Avro container file with records. Each record has exactly
 * two fields, 'key' and 'value'. The keys are sorted lexicographically. The 'index' file
 * is a small Avro container file mapping keys in the 'data' file to their byte
 * positions. The index file is intended to fit in memory, so it should remain
 * small. There is one entry in the index file for each data block in the Avro container
 * file.</p>
 *
 * <p>SortedKeyValueFile is to Avro container file as MapFile is to SequenceFile.</p>
 */
public class SortedKeyValueFile {
  private static final Logger LOG = LoggerFactory.getLogger(SortedKeyValueFile.class);

  /** The name of the data file within the SortedKeyValueFile directory. */
  public static final String DATA_FILENAME = "data";

  /** The name of the index file within the SortedKeyValueFile directory. */
  public static final String INDEX_FILENAME = "index";

  /**
   * Reads a SortedKeyValueFile.
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class Reader<K, V> {
  }

  /**
   * Writes a SortedKeyValueFile.
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class Writer<K, V> implements Closeable {
    /** The key schema. */
    private final Schema mKeySchema;

    /** The value schema. */
    private final Schema mValueSchema;

    /** The schema of the data file records. */
    private final Schema mRecordSchema;

    /** The schema of the index file records. */
    private final Schema mIndexSchema;

    /** The writer for the data file. */
    private final DataFileWriter mDataFileWriter;

    /** The writer for the index file. */
    private final DataFileWriter mIndexFileWriter;

    /** We store an indexed key for every mIndexInterval records written to the data file. */
    private final int mIndexInterval;

    /** The number of records written to the file so far. */
    private long mRecordsWritten;

    /** The most recent key that was appended to the file, or null. */
    private K mPreviousKey;

    /** A class to encapsulate the various options of a SortedKeyValueFile.Writer. */
    public static class Options {
      /** The key schema. */
      private Schema mKeySchema;

      /** The value schema. */
      private Schema mValueSchema;

      /** The configuration. */
      private Configuration mConf;

      /** The path to the output file. */
      private Path mPath;

      /** The number of records between indexed entries. */
      private int mIndexInterval = 128;

      /**
       * Sets the key schema.
       *
       * @param keySchema The key schema.
       * @return This options instance.
       */
      public Options withKeySchema(Schema keySchema) {
        mKeySchema = keySchema;
        return this;
      }

      /**
       * Gets the key schema.
       *
       * @return The key schema.
       */
      public Schema getKeySchema() {
        return mKeySchema;
      }

      /**
       * Sets the value schema.
       *
       * @param valueSchema The value schema.
       * @return This options instance.
       */
      public Options withValueSchema(Schema valueSchema) {
        mValueSchema = valueSchema;
        return this;
      }

      /**
       * Gets the value schema.
       *
       * @return The value schema.
       */
      public Schema getValueSchema() {
        return mValueSchema;
      }

      /**
       * Sets the configuration.
       *
       * @param conf The configuration.
       * @return This options instance.
       */
      public Options withConfiguration(Configuration conf) {
        mConf = conf;
        return this;
      }

      /**
       * Gets the configuration.
       *
       * @return The configuration.
       */
      public Configuration getConfiguration() {
        return mConf;
      }

      /**
       * Sets the output path.
       *
       * @param path The output path.
       * @return This options instance.
       */
      public Options withPath(Path path) {
        mPath = path;
        return this;
      }

      /**
       * Gets the output path.
       *
       * @return The output path.
       */
      public Path getPath() {
        return mPath;
      }

      /**
       * Sets the index interval.
       *
       * <p>If the index inverval is N, then every N records will be indexed into the
       * index file.</p>
       *
       * @param indexInterval The index interval.
       * @return This options instance.
       */
      public Options withIndexInterval(int indexInterval) {
        mIndexInterval = indexInterval;
        return this;
      }

      /**
       * Gets the index interval.
       *
       * @return The index interval.
       */
      public int getIndexInterval() {
        return mIndexInterval;
      }
    }

    /**
     * Creates a writer for a new file.
     *
     * @param options The options.
     * @throws IOException If there is an error.
     */
    public Writer(Options options) throws IOException {
      FileSystem fileSystem = options.getPath().getFileSystem(options.getConfiguration());

      // Save the key and value schemas.
      mKeySchema = options.getKeySchema();
      mValueSchema = options.getValueSchema();

      // Save the index interval.
      mIndexInterval = options.getIndexInterval();

      // Create the directory.
      if (!fileSystem.mkdirs(options.getPath())) {
        throw new IOException(
            "Unable to create directory for SortedKeyValueFile: " + options.getPath());
      }
      LOG.debug("Created directory " + options.getPath());

      // Open a writer for the data file.
      Path dataFilePath = new Path(options.getPath(), DATA_FILENAME);
      LOG.debug("Creating writer for avro data file: " + dataFilePath);
      mRecordSchema = AvroKeyValue.getSchema(mKeySchema, mValueSchema);
      DatumWriter datumWriter = new GenericDatumWriter<GenericRecord>(mRecordSchema);
      OutputStream dataOutputStream = fileSystem.create(dataFilePath);
      mDataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)
          .setSyncInterval(1 << 20)  // Set the auto-sync interval sufficiently large, since
                                     // we will manually sync every mIndexInterval records.
          .create(mRecordSchema, dataOutputStream);

      // Open a writer for the index file.
      Path indexFilePath = new Path(options.getPath(), INDEX_FILENAME);
      LOG.debug("Creating writer for avro index file: " + indexFilePath);
      mIndexSchema = AvroKeyValue.getSchema(mKeySchema, Schema.create(Schema.Type.LONG));
      DatumWriter indexWriter = new GenericDatumWriter<GenericRecord>(mIndexSchema);
      OutputStream indexOutputStream = fileSystem.create(indexFilePath);
      mIndexFileWriter = new DataFileWriter<GenericRecord>(indexWriter)
          .create(mIndexSchema, indexOutputStream);
    }

    /**
     * Appends a record to the SortedKeyValueFile.
     *
     * @param key The key.
     * @param value The value.
     * @throws IOException If there is an error.
     */
    public void append(K key, V value) throws IOException {
      // Make sure the keys are inserted in sorted order.
      if (null != mPreviousKey && GenericData.get().compare(key, mPreviousKey, mKeySchema) < 0) {
        throw new IllegalArgumentException("Records must be inserted in sorted key order."
            + " Attempted to insert key " + key + " after " + mPreviousKey + ".");
      }
      mPreviousKey = key;

      // Construct the data record.
      AvroKeyValue<K, V> dataRecord
          = new AvroKeyValue<K, V>(new GenericData.Record(mRecordSchema));
      dataRecord.setKey(key);
      dataRecord.setValue(value);

      // Index it if necessary.
      if (0 == mRecordsWritten++ % mIndexInterval) {
        // Force a sync to the data file writer, which closes the current data block (if
        // nonempty) and reports the current position in the file.
        long position = mDataFileWriter.sync();

        // Construct the record to put in the index.
        AvroKeyValue<K, Long> indexRecord
            = new AvroKeyValue<K, Long>(new GenericData.Record(mIndexSchema));
        indexRecord.setKey(key);
        indexRecord.setValue(position);
        mIndexFileWriter.append(indexRecord.get());
      }

      // Write it to the data file.
      mDataFileWriter.append(dataRecord.get());
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mIndexFileWriter.close();
      mDataFileWriter.close();
    }
  }
}
