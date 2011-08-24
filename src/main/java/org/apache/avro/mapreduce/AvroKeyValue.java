// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.mapreduce;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

/**
 * A helper object for working with the Avro generic records that are used to store key/value
 * pairs in an Avro container file.
 *
 * @param <K> The java type for the key.
 * @param <V> The java type for the value.
 */
public class AvroKeyValue<K, V> {
  /** The name of the key value pair generic record. */
  public static final String KEY_VALUE_PAIR_RECORD_NAME = "KeyValuePair";

  /** The namespace of the key value pair generic record. */
  public static final String KEY_VALUE_PAIR_RECORD_NAMESPACE = "org.apache.avro.mapreduce";

  /** The name of the generic record field containing the key. */
  public static final String KEY_FIELD = "key";

  /** The name of the generic record field containing the value. */
  public static final String VALUE_FIELD = "value";

  /** The key/value generic record wrapped by this class. */
  private final GenericRecord mKeyValueRecord;

  /**
   * Wraps a GenericRecord that is a key value pair.
   */
  public AvroKeyValue(GenericRecord keyValueRecord) {
    mKeyValueRecord = keyValueRecord;
  }

  /**
   * Gets the wrapped key/value GenericRecord.
   *
   * @return The key/value Avro generic record.
   */
  public GenericRecord get() {
    return mKeyValueRecord;
  }

  /**
   * Read the key.
   *
   * @return The key from the key/value generic record.
   */
  public K getKey() {
    return (K) mKeyValueRecord.get(KEY_FIELD);
  }

  /**
   * Read the value.
   *
   * @return The value from the key/value generic record.
   */
  public V getValue() {
    return (V) mKeyValueRecord.get(VALUE_FIELD);
  }

  /**
   * Sets the key.
   *
   * @param key The key.
   */
  public void setKey(K key) {
    mKeyValueRecord.put(KEY_FIELD, key);
  }

  /**
   * Sets the value.
   *
   * @param value The value.
   */
  public void setValue(V value) {
    mKeyValueRecord.put(VALUE_FIELD, value);
  }

  /**
   * Creates a KeyValuePair generic record schema.
   *
   * @return A schema for a generic record with two fields: 'key' and 'value'.
   */
  public static Schema getSchema(Schema keySchema, Schema valueSchema) {
    Schema schema = Schema.createRecord(
        KEY_VALUE_PAIR_RECORD_NAME, "A key/value pair", KEY_VALUE_PAIR_RECORD_NAMESPACE, false);
    schema.setFields(Arrays.asList(
        new Schema.Field(KEY_FIELD, keySchema, "The key", null),
        new Schema.Field(VALUE_FIELD, valueSchema, "The value", null)));
    return schema;
  }
}
