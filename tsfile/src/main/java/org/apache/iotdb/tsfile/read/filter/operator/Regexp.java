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
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Regexp.
 *
 * @param <T> comparable data type
 */
public class Regexp<T extends Comparable<T>> implements Filter, Serializable {

  private static final long serialVersionUID = -1168073851950524983L;

  protected String value;

  protected FilterType filterType;

  protected Pattern pattern;

  protected boolean not;

  public Regexp() {}

  public Regexp(String value, FilterType filterType, boolean not) {
    this.value = value;
    this.filterType = filterType;
    this.not = not;
    try {
      this.pattern = Pattern.compile(this.value);
    } catch (PatternSyntaxException e) {
      throw new PatternSyntaxException("Regular expression error", value, e.getIndex());
    }
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
  public boolean satisfy(long time, Object value) {
    if (filterType != FilterType.VALUE_FILTER) {
      throw new UnsupportedOperationException("");
    }
    return not != pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return true;
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return false;
  }

  @Override
  public Filter copy() {
    return new Regexp<>(value, filterType, not);
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.write(value, outputStream);
      ReadWriteIOUtils.write(not, outputStream);
    } catch (IOException ignored) {
      // ignore
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    value = ReadWriteIOUtils.readString(buffer);
    not = ReadWriteIOUtils.readBool(buffer);
    if (value != null) {
      try {
        this.pattern = Pattern.compile(value);
      } catch (PatternSyntaxException e) {
        throw new PatternSyntaxException("Regular expression error", value, e.getIndex());
      }
    }
  }

  @Override
  public String toString() {
    return filterType + (not ? " not match " : " match ") + value;
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.REGEXP;
  }

  @Override
  public Filter reverse() {
    return new Regexp<>(value, filterType, !not);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Regexp)) {
      return false;
    }
    Regexp<?> regexp = (Regexp<?>) o;
    return not == regexp.not && value.equals(regexp.value) && filterType == regexp.filterType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, filterType, not);
  }

  public static class AccessCount {
    private int count;
    private final int accessThreshold =
        TSFileDescriptor.getInstance().getConfig().getPatternMatchingThreshold();

    public void check() throws IllegalStateException {
      if (this.count++ > accessThreshold) {
        throw new IllegalStateException("Pattern access threshold exceeded");
      }
    }
  }

  public static class MatcherInput implements CharSequence {

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
