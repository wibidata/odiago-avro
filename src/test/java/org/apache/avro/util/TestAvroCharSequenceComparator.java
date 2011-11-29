// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.util;

import static org.junit.Assert.*;
import static org.hamcrest.number.OrderingComparisons.*;

import org.junit.Before;
import org.junit.Test;

public class TestAvroCharSequenceComparator {
  private AvroCharSequenceComparator<CharSequence> mComparator;

  @Before
  public void setup() {
    mComparator = new AvroCharSequenceComparator<CharSequence>();
  }

  @Test
  public void testCompareString() {
    assertEquals(0, mComparator.compare("", ""));
    assertThat(mComparator.compare("", "a"), lessThan(0));
    assertThat(mComparator.compare("a", ""), greaterThan(0));

    assertEquals(0, mComparator.compare("a", "a"));
    assertThat(mComparator.compare("a", "b"), lessThan(0));
    assertThat(mComparator.compare("b", "a"), greaterThan(0));

    assertEquals(0, mComparator.compare("ab", "ab"));
    assertThat(mComparator.compare("a", "aa"), lessThan(0));
    assertThat(mComparator.compare("aa", "a"), greaterThan(0));

    assertThat(mComparator.compare("abc", "abcdef"), lessThan(0));
    assertThat(mComparator.compare("abcdef", "abc"), greaterThan(0));
  }

  @Test
  public void testCompareUtf8() {
    assertEquals(0, mComparator.compare(new Utf8(""), new Utf8("")));
    assertThat(mComparator.compare(new Utf8(""), new Utf8("a")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("")), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("a"), new Utf8("a")));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("b")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("b"), new Utf8("a")), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("ab"), new Utf8("ab")));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("aa")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("aa"), new Utf8("a")), greaterThan(0));

    assertThat(mComparator.compare(new Utf8("abc"), new Utf8("abcdef")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("abcdef"), new Utf8("abc")), greaterThan(0));
  }

  @Test
  public void testCompareUtf8ToString() {
    assertEquals(0, mComparator.compare(new Utf8(""), ""));
    assertThat(mComparator.compare(new Utf8(""), "a"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("a"), ""), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("a"), "a"));
    assertThat(mComparator.compare(new Utf8("a"), "b"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("b"), "a"), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("ab"), "ab"));
    assertThat(mComparator.compare(new Utf8("a"), "aa"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("aa"), "a"), greaterThan(0));

    assertThat(mComparator.compare(new Utf8("abc"), "abcdef"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("abcdef"), "abc"), greaterThan(0));
  }
}
