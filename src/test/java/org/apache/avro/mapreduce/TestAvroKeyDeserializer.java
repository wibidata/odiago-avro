// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.junit.Test;

public class TestAvroKeyDeserializer {
  @Test
  public void testDeserialize() throws IOException {
    // Create a deserializer.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    AvroKeyDeserializer<CharSequence> deserializer
        = new AvroKeyDeserializer<CharSequence>(readerSchema);

    // Check the reader schema.
    assertEquals(readerSchema, deserializer.getReaderSchema());

    // Write some records to deserialize.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    DatumWriter<CharSequence> datumWriter = new GenericDatumWriter<CharSequence>(writerSchema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write("record1", encoder);
    datumWriter.write("record2", encoder);
    encoder.flush();

    // Deserialize the records.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    deserializer.open(inputStream);
    AvroKey<CharSequence> record = null;

    record = deserializer.deserialize(record);
    assertEquals("record1", record.datum().toString());

    record = deserializer.deserialize(record);
    assertEquals("record2", record.datum().toString());

    deserializer.close();
  }
}
