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

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnCompareFilter;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnPatternMatchFilter;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnRangeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.ColumnSetFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilter;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * These are the value column operators in a filter predicate expression tree. They are constructed
 * by using the methods in {@link ValueFilter}
 */
public final class ValueFilterOperators {

  private ValueFilterOperators() {
    // forbidden construction
  }

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter<T extends Comparable<T>>
      extends ColumnCompareFilter<T> {

    protected final String measurement;
    private final String toString;

    protected ValueColumnCompareFilter(String measurement, T constant) {
      super(constant);
      this.measurement = Objects.requireNonNull(measurement, "measurement cannot be null");

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(" + measurement + ", " + constant + ")";
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return false;
    }
  }

  public static final class ValueEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant can be null
    public ValueEq(String measurement, T constant) {
      super(measurement, constant);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) >= 0
          && constant.compareTo((T) statistics.getMaxValue()) <= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.equals(value);
    }

    @Override
    public Filter reverse() {
      return new ValueNotEq<>(measurement, constant);
    }
  }

  public static final class ValueNotEq<T extends Comparable<T>>
      extends ValueColumnCompareFilter<T> {

    // constant can be null
    public ValueNotEq(String measurement, T constant) {
      super(measurement, constant);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return !(constant.compareTo((T) statistics.getMinValue()) == 0
          && constant.compareTo((T) statistics.getMaxValue()) == 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) < 0
          || constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !constant.equals(value);
    }

    @Override
    public Filter reverse() {
      return new ValueEq<>(measurement, constant);
    }
  }

  public static final class ValueLt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueLt(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, "constant cannot be null"));
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) > 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMaxValue()) > 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) > 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGtEq<>(measurement, constant);
    }
  }

  public static final class ValueLtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueLtEq(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, "constant cannot be null"));
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMinValue()) >= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMaxValue()) >= 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) >= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueGt<>(measurement, constant);
    }
  }

  public static final class ValueGt<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueGt(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, "constant cannot be null"));
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMaxValue()) < 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) < 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) < 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLtEq<>(measurement, constant);
    }
  }

  public static final class ValueGtEq<T extends Comparable<T>> extends ValueColumnCompareFilter<T> {

    // constant cannot be null
    public ValueGtEq(String measurement, T constant) {
      super(measurement, Objects.requireNonNull(constant, "constant cannot be null"));
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return constant.compareTo((T) statistics.getMaxValue()) <= 0;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return constant.compareTo((T) statistics.getMinValue()) <= 0;
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return constant.compareTo((T) value) <= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueLt<>(measurement, constant);
    }
  }

  // base class for ValueBetweenAnd, ValueNotBetweenAnd
  abstract static class ValueColumnRangeFilter<T extends Comparable<T>>
      extends ColumnRangeFilter<T> {

    protected final String measurement;
    private final String toString;

    protected ValueColumnRangeFilter(String measurement, T min, T max) {
      super(min, max);
      this.measurement = Objects.requireNonNull(measurement, "measurement cannot be null");

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(" + measurement + ", " + min + ", " + max + ")";
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return false;
    }
  }

  public static final class ValueBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueBetweenAnd(String measurement, T min, T max) {
      super(measurement, min, max);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return (((T) statistics.getMaxValue()).compareTo(min) >= 0
          && ((T) statistics.getMinValue()).compareTo(max) <= 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return (((T) statistics.getMinValue()).compareTo(min) >= 0
          && ((T) statistics.getMaxValue()).compareTo(max) <= 0);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return min.compareTo((T) value) <= 0 && max.compareTo((T) value) >= 0;
    }

    @Override
    public Filter reverse() {
      return new ValueNotBetweenAnd<>(measurement, min, max);
    }
  }

  public static final class ValueNotBetweenAnd<T extends Comparable<T>>
      extends ValueColumnRangeFilter<T> {

    public ValueNotBetweenAnd(String measurement, T min, T max) {
      super(measurement, min, max);
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return true;
      }
      return (((T) statistics.getMinValue()).compareTo(min) < 0
          || ((T) statistics.getMaxValue()).compareTo(max) > 0);
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      if (statistics.getType() == TSDataType.TEXT || statistics.getType() == TSDataType.BOOLEAN) {
        return false;
      }
      return (((T) statistics.getMinValue()).compareTo(max) > 0
          || ((T) statistics.getMaxValue()).compareTo(min) < 0);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return min.compareTo((T) value) > 0 || max.compareTo((T) value) < 0;
    }

    @Override
    public Filter reverse() {
      return new ValueBetweenAnd<>(measurement, min, max);
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter<T extends Comparable<T>> extends ColumnSetFilter<T> {

    protected final String measurement;
    private final String toString;

    protected ValueColumnSetFilter(String measurement, Set<T> candidates) {
      super(candidates);
      this.measurement = Objects.requireNonNull(measurement, "measurement cannot be null");

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(" + measurement + ", " + candidates + ")";
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return true;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return false;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return false;
    }
  }

  public static final class ValueIn<T extends Comparable<T>> extends ValueColumnSetFilter<T> {

    public ValueIn(String measurement, Set<T> candidates) {
      super(measurement, candidates);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return candidates.contains((T) value);
    }

    @Override
    public Filter reverse() {
      return new ValueNotIn<>(measurement, candidates);
    }
  }

  public static final class ValueNotIn<T extends Comparable<T>> extends ValueColumnSetFilter<T> {

    public ValueNotIn(String measurement, Set<T> candidates) {
      super(measurement, candidates);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !candidates.contains((T) value);
    }

    @Override
    public Filter reverse() {
      return new ValueIn<>(measurement, candidates);
    }
  }

  // base class for ValueLike, ValueNotLike, ValueRegex, ValueNotRegex
  abstract static class ValueColumnPatternMatchFilter extends ColumnPatternMatchFilter {

    protected final String measurement;
    private final String toString;

    protected ValueColumnPatternMatchFilter(String measurement, String regex) {
      super(regex);
      this.measurement = Objects.requireNonNull(measurement, "measurement cannot be null");

      String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
      this.toString = name + "(" + measurement + ", " + regex + ")";
    }

    public String getMeasurement() {
      return measurement;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
      return true;
    }

    @Override
    public boolean allSatisfy(Statistics statistics) {
      return false;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return true;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return false;
    }
  }

  public static final class ValueLike extends ValueColumnPatternMatchFilter {

    public ValueLike(String measurement, String regex) {
      super(measurement, parseLikePatternToRegex(regex));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return pattern.matcher(value.toString()).find();
    }

    @Override
    public Filter reverse() {
      return new ValueNotLike(measurement, pattern.pattern());
    }
  }

  public static final class ValueNotLike extends ValueColumnPatternMatchFilter {

    public ValueNotLike(String measurement, String likePattern) {
      super(measurement, parseLikePatternToRegex(likePattern));
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !pattern.matcher(value.toString()).find();
    }

    @Override
    public Filter reverse() {
      return new ValueLike(measurement, pattern.pattern());
    }
  }

  private static String parseLikePatternToRegex(String value) {
    String unescapeValue = unescapeString(value);
    String specialRegexStr = ".^$*+?{}[]|()";
    StringBuilder patternStrBuild = new StringBuilder();
    patternStrBuild.append("^");
    for (int i = 0; i < unescapeValue.length(); i++) {
      String ch = String.valueOf(unescapeValue.charAt(i));
      if (specialRegexStr.contains(ch)) {
        ch = "\\" + unescapeValue.charAt(i);
      }
      if ((i == 0)
          || (i > 0 && !"\\".equals(String.valueOf(unescapeValue.charAt(i - 1))))
          || (i >= 2
              && "\\\\"
                  .equals(
                      patternStrBuild.substring(
                          patternStrBuild.length() - 2, patternStrBuild.length())))) {
        String replaceStr = ch.replace("%", ".*?").replace("_", ".");
        patternStrBuild.append(replaceStr);
      } else {
        patternStrBuild.append(ch);
      }
    }
    patternStrBuild.append("$");
    return patternStrBuild.toString();
  }

  /**
   * This Method is for unescaping strings except '\' before special string '%', '_', '\', because
   * we need to use '\' to judge whether to replace this to regexp string
   */
  private static String unescapeString(String value) {
    StringBuilder out = new StringBuilder();
    int curIndex = 0;
    for (; curIndex < value.length(); curIndex++) {
      String ch = String.valueOf(value.charAt(curIndex));
      if ("\\".equals(ch)) {
        if (curIndex < value.length() - 1) {
          String nextChar = String.valueOf(value.charAt(curIndex + 1));
          if ("%".equals(nextChar) || "_".equals(nextChar) || "\\".equals(nextChar)) {
            out.append(ch);
          }
          if ("\\".equals(nextChar)) {
            curIndex++;
          }
        }
      } else {
        out.append(ch);
      }
    }
    return out.toString();
  }

  public static final class ValueRegexp extends ValueColumnPatternMatchFilter {

    public ValueRegexp(String measurement, String regex) {
      super(measurement, regex);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueNotRegexp(measurement, pattern.pattern());
    }
  }

  public static final class ValueNotRegexp extends ValueColumnPatternMatchFilter {

    public ValueNotRegexp(String measurement, String regex) {
      super(measurement, regex);
    }

    @Override
    public boolean satisfy(long time, Object value) {
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public Filter reverse() {
      return new ValueRegexp(measurement, pattern.pattern());
    }
  }

  private static class AccessCount {
    private int count;
    private final int accessThreshold =
        TSFileDescriptor.getInstance().getConfig().getPatternMatchingThreshold();

    public void check() throws IllegalStateException {
      if (this.count++ > accessThreshold) {
        throw new IllegalStateException("Pattern access threshold exceeded");
      }
    }
  }

  private static class MatcherInput implements CharSequence {

    private final CharSequence value;

    private final AccessCount access;

    public MatcherInput(CharSequence value, AccessCount access) {
      this.value = value;
      this.access = access;
    }

    @Override
    public char charAt(int index) {
      this.access.check();
      return this.value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new MatcherInput(this.value.subSequence(start, end), this.access);
    }

    @Override
    public int length() {
      return this.value.length();
    }

    @Override
    public String toString() {
      return this.value.toString();
    }
  }
}
