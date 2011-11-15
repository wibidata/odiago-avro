// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSortedKeyValueFile {
  private static final Logger LOG = LoggerFactory.getLogger(TestSortedKeyValueFile.class);

  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Test(expected=IllegalArgumentException.class)
  public void testWriteOutOfSortedOrder() throws IOException {
    LOG.debug("Writing some records to a SortedKeyValueFile...");

    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING))
        .withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf)
        .withPath(new Path(mTempDir.getRoot().getPath(), "myfile"))
        .withIndexInterval(2);  // Index every other record.

    SortedKeyValueFile.Writer<CharSequence, CharSequence> writer
        = new SortedKeyValueFile.Writer<CharSequence, CharSequence>(options);

    try {
      writer.append("banana", "Banana");
      writer.append("apple", "Apple");  // Ruh, roh!
    } finally {
      writer.close();
    }
  }

  @Test
  public void testWriter() throws IOException {
    LOG.debug("Writing some records to a SortedKeyValueFile...");

    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING))
        .withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf)
        .withPath(new Path(mTempDir.getRoot().getPath(), "myfile"))
        .withIndexInterval(2);  // Index every other record.

    SortedKeyValueFile.Writer<CharSequence, CharSequence> writer
        = new SortedKeyValueFile.Writer<CharSequence, CharSequence>(options);

    try {
      writer.append("apple", "Apple");  // Will be indexed.
      writer.append("banana", "Banana");
      writer.append("carrot", "Carrot");  // Will be indexed.
      writer.append("durian", "Durian");
    } finally {
      writer.close();
    }


    LOG.debug("Checking the generated directory...");
    File directory = new File(mTempDir.getRoot().getPath(), "myfile");
    assertTrue(directory.exists());


    LOG.debug("Checking the generated index file...");
    File indexFile = new File(directory, SortedKeyValueFile.INDEX_FILENAME);
    DatumReader<GenericRecord> indexReader = new GenericDatumReader<GenericRecord>(
        AvroKeyValue.getSchema(options.getKeySchema(), Schema.create(Schema.Type.LONG)));
    FileReader<GenericRecord> indexFileReader = DataFileReader.openReader(indexFile, indexReader);

    List<AvroKeyValue<CharSequence, Long>> indexRecords
        = new ArrayList<AvroKeyValue<CharSequence, Long>>();
    for (GenericRecord indexRecord : indexFileReader) {
      indexRecords.add(new AvroKeyValue<CharSequence, Long>(indexRecord));
    }
    assertEquals(2, indexRecords.size());
    assertEquals("apple", indexRecords.get(0).getKey().toString());
    LOG.debug("apple's position in the file: " + indexRecords.get(0).getValue());
    assertEquals("carrot", indexRecords.get(1).getKey().toString());
    LOG.debug("carrot's position in the file: " + indexRecords.get(1).getValue());


    LOG.debug("Checking the generated data file...");
    File dataFile = new File(directory, SortedKeyValueFile.DATA_FILENAME);
    DatumReader<GenericRecord> dataReader = new GenericDatumReader<GenericRecord>(
        AvroKeyValue.getSchema(options.getKeySchema(), options.getValueSchema()));
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader(dataFile, dataReader);

    dataFileReader.seek(indexRecords.get(0).getValue());
    assertTrue(dataFileReader.hasNext());
    AvroKeyValue<CharSequence, CharSequence> appleRecord
        = new AvroKeyValue<CharSequence, CharSequence>(dataFileReader.next());
    assertEquals("apple", appleRecord.getKey().toString());
    assertEquals("Apple", appleRecord.getValue().toString());

    dataFileReader.seek(indexRecords.get(1).getValue());
    assertTrue(dataFileReader.hasNext());
    AvroKeyValue<CharSequence, CharSequence> carrotRecord
        = new AvroKeyValue<CharSequence, CharSequence>(dataFileReader.next());
    assertEquals("carrot", carrotRecord.getKey().toString());
    assertEquals("Carrot", carrotRecord.getValue().toString());

    assertTrue(dataFileReader.hasNext());
    AvroKeyValue<CharSequence, CharSequence> durianRecord
        = new AvroKeyValue<CharSequence, CharSequence>(dataFileReader.next());
    assertEquals("durian", durianRecord.getKey().toString());
    assertEquals("Durian", durianRecord.getValue().toString());
  }
}
