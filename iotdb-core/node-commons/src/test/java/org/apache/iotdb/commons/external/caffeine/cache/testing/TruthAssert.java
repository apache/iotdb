/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.external.caffeine.cache.testing;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** Lightweight fluent assertion helper for ported Caffeine unit tests on JUnit 4. */
public final class TruthAssert {

  private TruthAssert() {}

  public static SubjectBuilder assertWithMessage(String format, Object... args) {
    return new SubjectBuilder(String.format(format, args));
  }

  public static ObjectSubject assertThat(Object actual) {
    return new ObjectSubject(null, actual);
  }

  public static BooleanSubject assertThat(Boolean actual) {
    return new BooleanSubject(null, actual);
  }

  public static LongSubject assertThat(Integer actual) {
    return new LongSubject(null, actual == null ? null : actual.longValue());
  }

  public static LongSubject assertThat(Long actual) {
    return new LongSubject(null, actual);
  }

  public static DoubleSubject assertThat(Double actual) {
    return new DoubleSubject(null, actual);
  }

  public static StringSubject assertThat(String actual) {
    return new StringSubject(null, actual);
  }

  public static OptionalSubject assertThat(Optional<?> actual) {
    return new OptionalSubject(null, actual);
  }

  public static OptionalIntSubject assertThat(OptionalInt actual) {
    return new OptionalIntSubject(null, actual);
  }

  public static OptionalLongSubject assertThat(OptionalLong actual) {
    return new OptionalLongSubject(null, actual);
  }

  public static IterableSubject assertThat(Iterable<?> actual) {
    return new IterableSubject(null, actual);
  }

  public static MapSubject assertThat(Map<?, ?> actual) {
    return new MapSubject(null, actual);
  }

  public static ObjectArraySubject assertThat(Object[] actual) {
    return new ObjectArraySubject(null, actual);
  }

  public static LongArraySubject assertThat(long[] actual) {
    return new LongArraySubject(null, actual);
  }

  public static ThrowableSubject assertThat(Throwable actual) {
    return new ThrowableSubject(null, actual);
  }

  public interface Ordered {
    void inOrder();
  }

  public static final class SubjectBuilder {
    private final String message;

    SubjectBuilder(String message) {
      this.message = message;
    }

    public ObjectSubject that(Object actual) {
      return new ObjectSubject(message, actual);
    }

    public BooleanSubject that(Boolean actual) {
      return new BooleanSubject(message, actual);
    }

    public LongSubject that(Integer actual) {
      return new LongSubject(message, actual == null ? null : actual.longValue());
    }

    public LongSubject that(Long actual) {
      return new LongSubject(message, actual);
    }

    public DoubleSubject that(Double actual) {
      return new DoubleSubject(message, actual);
    }

    public StringSubject that(String actual) {
      return new StringSubject(message, actual);
    }

    public IterableSubject that(Iterable<?> actual) {
      return new IterableSubject(message, actual);
    }

    public MapSubject that(Map<?, ?> actual) {
      return new MapSubject(message, actual);
    }
  }

  public static class ObjectSubject {
    protected final String message;
    protected final Object actual;

    ObjectSubject(String message, Object actual) {
      this.message = message;
      this.actual = actual;
    }

    protected String msg(String detail) {
      return message == null ? detail : message + ": " + detail;
    }

    private static boolean numericEquals(Object a, Object b) {
      if (Objects.equals(a, b)) {
        return true;
      }
      if ((a instanceof Integer || a instanceof Long)
          && (b instanceof Integer || b instanceof Long)) {
        return ((Number) a).longValue() == ((Number) b).longValue();
      }
      if ((a instanceof Double || a instanceof Float) && b instanceof Number) {
        return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
      }
      if ((b instanceof Double || b instanceof Float) && a instanceof Number) {
        return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
      }
      return false;
    }

    public void isEqualTo(Object expected) {
      if (numericEquals(actual, expected)) {
        return;
      }
      Assert.assertEquals(message, expected, actual);
    }

    public void isNotEqualTo(Object unexpected) {
      if (numericEquals(actual, unexpected)) {
        Assert.fail(msg("expected not to be equal to " + unexpected));
      }
      Assert.assertNotEquals(message, unexpected, actual);
    }

    public void isNull() {
      Assert.assertNull(message, actual);
    }

    public void isNotNull() {
      Assert.assertNotNull(message, actual);
    }

    public void isSameInstanceAs(Object expected) {
      Assert.assertSame(message, expected, actual);
    }

    public void isNotSameInstanceAs(Object unexpected) {
      Assert.assertNotSame(message, unexpected, actual);
    }

    public void isInstanceOf(Class<?> clazz) {
      Assert.assertNotNull(
          msg("expected instance of " + clazz.getName() + " but was null"), actual);
      Assert.assertTrue(
          msg(
              "expected instance of "
                  + clazz.getName()
                  + " but was "
                  + actual.getClass().getName()),
          clazz.isInstance(actual));
    }

    public void isNotInstanceOf(Class<?> clazz) {
      if (actual != null) {
        Assert.assertFalse(
            msg("expected not to be instance of " + clazz.getName()), clazz.isInstance(actual));
      }
    }

    public void isAnyOf(Object first, Object second, Object... rest) {
      if (numericEquals(actual, first) || numericEquals(actual, second)) {
        return;
      }
      for (Object item : rest) {
        if (numericEquals(actual, item)) {
          return;
        }
      }
      Assert.fail(msg("expected " + actual + " to be one of the allowed values"));
    }
  }

  public static final class BooleanSubject extends ObjectSubject {
    BooleanSubject(String message, Boolean actual) {
      super(message, actual);
    }

    public void isTrue() {
      Assert.assertEquals(message, Boolean.TRUE, actual);
    }

    public void isFalse() {
      Assert.assertEquals(message, Boolean.FALSE, actual);
    }
  }

  public static final class LongSubject extends ObjectSubject {
    private final Long value;

    LongSubject(String message, Long actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isGreaterThan(long other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " > " + other), value > other);
    }

    public void isLessThan(long other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " < " + other), value < other);
    }

    public void isAtLeast(long other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " >= " + other), value >= other);
    }

    public void isAtMost(long other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " <= " + other), value <= other);
    }
  }

  public static final class DoubleSubject extends ObjectSubject {
    private final Double value;

    DoubleSubject(String message, Double actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isGreaterThan(double other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " > " + other), value > other);
    }

    public void isLessThan(double other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " < " + other), value < other);
    }

    public void isAtLeast(double other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " >= " + other), value >= other);
    }

    public void isAtMost(double other) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected " + value + " <= " + other), value <= other);
    }

    public TolerantDoubleComparison isWithin(double tolerance) {
      Assert.assertNotNull(message, value);
      return expected -> Assert.assertEquals(message, expected, value, tolerance);
    }
  }

  public interface TolerantDoubleComparison {
    void of(double expected);
  }

  public static final class StringSubject extends ObjectSubject {
    private final String value;

    StringSubject(String message, String actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isEmpty() {
      Assert.assertEquals(message, "", value);
    }

    public void contains(CharSequence sub) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(
          msg("expected '" + value + "' to contain '" + sub + "'"), value.contains(sub));
    }

    public void startsWith(String prefix) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(
          msg("expected '" + value + "' to start with '" + prefix + "'"), value.startsWith(prefix));
    }
  }

  public static final class OptionalSubject extends ObjectSubject {
    private final Optional<?> value;

    OptionalSubject(String message, Optional<?> actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isPresent() {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected Optional to be present"), value.isPresent());
    }

    public void isEmpty() {
      Assert.assertNotNull(message, value);
      Assert.assertFalse(msg("expected Optional to be empty, was " + value), value.isPresent());
    }

    public void hasValue(Object expected) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected Optional to have value " + expected), value.isPresent());
      Assert.assertEquals(message, expected, value.get());
    }
  }

  public static final class OptionalIntSubject extends ObjectSubject {
    private final OptionalInt value;

    OptionalIntSubject(String message, OptionalInt actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isPresent() {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected OptionalInt to be present"), value.isPresent());
    }

    public void isEmpty() {
      Assert.assertNotNull(message, value);
      Assert.assertFalse(msg("expected OptionalInt to be empty"), value.isPresent());
    }

    public void hasValue(int expected) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected OptionalInt to be present"), value.isPresent());
      Assert.assertEquals(message, expected, value.getAsInt());
    }
  }

  public static final class OptionalLongSubject extends ObjectSubject {
    private final OptionalLong value;

    OptionalLongSubject(String message, OptionalLong actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isPresent() {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected OptionalLong to be present"), value.isPresent());
    }

    public void isEmpty() {
      Assert.assertNotNull(message, value);
      Assert.assertFalse(msg("expected OptionalLong to be empty"), value.isPresent());
    }

    public void hasValue(long expected) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected OptionalLong to be present"), value.isPresent());
      Assert.assertEquals(message, expected, value.getAsLong());
    }
  }

  public static final class IterableSubject extends ObjectSubject {
    private final Iterable<?> value;

    IterableSubject(String message, Iterable<?> actual) {
      super(message, actual);
      this.value = actual;
    }

    private List<Object> toList() {
      Assert.assertNotNull(message, value);
      List<Object> list = new ArrayList<>();
      for (Object e : value) {
        list.add(e);
      }
      return list;
    }

    public void isEmpty() {
      Assert.assertNotNull(message, value);
      if (value instanceof Collection<?>) {
        Collection<?> c = (Collection<?>) value;
        Assert.assertTrue(msg("expected empty collection, size=" + c.size()), c.isEmpty());
        return;
      }
      List<Object> list = toList();
      Assert.assertTrue(msg("expected empty iterable, was " + list), list.isEmpty());
    }

    public void isExhaustivelyEmpty() {
      isEmpty();
      if (value instanceof Collection<?>) {
        Collection<?> c = (Collection<?>) value;
        Assert.assertTrue(c.isEmpty());
        Assert.assertEquals(0, c.size());
        Assert.assertFalse(c.iterator().hasNext());
        Assert.assertEquals(0, c.toArray().length);
      }
    }

    public void hasSize(int expected) {
      if (value instanceof Collection<?>) {
        Assert.assertEquals(message, expected, ((Collection<?>) value).size());
      } else {
        Assert.assertEquals(message, expected, toList().size());
      }
    }

    public void contains(Object element) {
      List<Object> list = toList();
      Assert.assertTrue(msg("expected " + list + " to contain " + element), list.contains(element));
    }

    public void doesNotContain(Object element) {
      List<Object> list = toList();
      Assert.assertFalse(
          msg("expected " + list + " not to contain " + element), list.contains(element));
    }

    public Ordered containsExactly(Object... expected) {
      List<Object> expectedList =
          expected == null ? Collections.singletonList(null) : Arrays.asList(expected);
      return containsExactlyElementsIn(expectedList);
    }

    public Ordered containsExactlyElementsIn(Iterable<?> expectedIterable) {
      List<Object> actualList = toList();
      List<Object> expectedList = new ArrayList<>();
      for (Object e : expectedIterable) {
        expectedList.add(e);
      }
      List<Object> remaining = new ArrayList<>(actualList);
      for (Object exp : expectedList) {
        if (!remaining.remove(exp)) {
          Assert.fail(msg("missing expected element " + exp + " in " + actualList));
        }
      }
      if (!remaining.isEmpty()) {
        Assert.fail(msg("unexpected extra elements " + remaining + " in " + actualList));
      }
      return () -> Assert.assertEquals(message, expectedList, actualList);
    }
  }

  public static final class MapSubject extends ObjectSubject {
    private final Map<?, ?> value;

    MapSubject(String message, Map<?, ?> actual) {
      super(message, actual);
      this.value = actual;
    }

    public void isEmpty() {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected empty map, was " + value), value.isEmpty());
    }

    public void isExhaustivelyEmpty() {
      isEmpty();
      Assert.assertEquals(0, value.size());
      Assert.assertTrue(value.keySet().isEmpty());
      Assert.assertTrue(value.values().isEmpty());
      Assert.assertTrue(value.entrySet().isEmpty());
    }

    public void hasSize(int expected) {
      Assert.assertNotNull(message, value);
      Assert.assertEquals(message, expected, value.size());
    }

    public void containsKey(Object key) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected map to contain key " + key), value.containsKey(key));
    }

    public void doesNotContainKey(Object key) {
      Assert.assertNotNull(message, value);
      Assert.assertFalse(msg("expected map not to contain key " + key), value.containsKey(key));
    }

    public void containsEntry(Object key, Object expectedValue) {
      Assert.assertNotNull(message, value);
      Assert.assertTrue(msg("expected map to contain key " + key), value.containsKey(key));
      Assert.assertEquals(message, expectedValue, value.get(key));
    }

    public Ordered containsExactly(Object k0, Object v0, Object... rest) {
      Map<Object, Object> expected = new HashMap<>();
      expected.put(k0, v0);
      for (int i = 0; i < rest.length; i += 2) {
        expected.put(rest[i], rest[i + 1]);
      }
      return containsExactlyEntriesIn(expected);
    }

    public Ordered containsExactlyEntriesIn(Map<?, ?> expected) {
      Assert.assertNotNull(message, value);
      Assert.assertEquals(message, expected, value);
      return () -> {
        Iterator<? extends Map.Entry<?, ?>> it1 = expected.entrySet().iterator();
        Iterator<? extends Map.Entry<?, ?>> it2 = value.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
          Assert.assertEquals(message, it1.next(), it2.next());
        }
        Assert.assertEquals(message, it1.hasNext(), it2.hasNext());
      };
    }
  }

  public static final class ObjectArraySubject extends ObjectSubject {
    private final Object[] value;

    ObjectArraySubject(String message, Object[] actual) {
      super(message, actual);
      this.value = actual;
    }

    public void hasLength(int expected) {
      Assert.assertNotNull(message, value);
      Assert.assertEquals(message, expected, value.length);
    }

    public void isEmpty() {
      hasLength(0);
    }

    public IterableSubject asList() {
      Assert.assertNotNull(message, value);
      return new IterableSubject(message, Arrays.asList(value));
    }
  }

  public static final class LongArraySubject extends ObjectSubject {
    private final long[] value;

    LongArraySubject(String message, long[] actual) {
      super(message, actual);
      this.value = actual;
    }

    public void hasLength(int expected) {
      Assert.assertNotNull(message, value);
      Assert.assertEquals(message, expected, value.length);
    }
  }

  public static final class ThrowableSubject extends ObjectSubject {
    private final Throwable value;

    ThrowableSubject(String message, Throwable actual) {
      super(message, actual);
      this.value = actual;
    }

    public StringSubject hasMessageThat() {
      Assert.assertNotNull(message, value);
      return new StringSubject(message, value.getMessage());
    }

    public ThrowableSubject hasCauseThat() {
      Assert.assertNotNull(message, value);
      return new ThrowableSubject(message, value.getCause());
    }
  }
}
