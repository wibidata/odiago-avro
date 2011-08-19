// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.junit.Test;

public class TestAvroSerializer {
  @Test
  public void testSerialize() throws IOException {
    // Create a serializer.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    AvroSerializer<CharSequence> serializer = new AvroSerializer<CharSequence>(writerSchema);

    // Check the writer schema.
    assertEquals(writerSchema, serializer.getWriterSchema());

    // Serialize two records, 'record1' and 'record2'.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    serializer.open(outputStream);
    serializer.serialize(new AvroKey<CharSequence>("record1"));
    serializer.serialize(new AvroKey<CharSequence>("record2"));
    serializer.close();

    // Make sure the records were serialized correctly.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    DatumReader<CharSequence> datumReader = new GenericDatumReader<CharSequence>(readerSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    CharSequence record = null;

    record = datumReader.read(record, decoder);
    assertEquals("record1", record.toString());

    record = datumReader.read(record, decoder);
    assertEquals("record2", record.toString());

    inputStream.close();
  }
}
