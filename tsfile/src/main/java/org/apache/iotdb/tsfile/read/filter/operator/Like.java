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
 * Like.
 *
 * @param <T> comparable data type
 */
public class Like<T extends Comparable<T>> implements Filter, Serializable {

  private static final long serialVersionUID = 2171102599229260789L;

  protected String value;

  protected FilterType filterType;

  protected Pattern pattern;

  protected boolean not;

  public Like() {}

  /**
   * The main idea of this part comes from
   * https://codereview.stackexchange.com/questions/36861/convert-sql-like-to-regex/36864
   *
   * @throws PatternSyntaxException if the regex expression's syntax is invalid
   */
  public Like(String value, FilterType filterType, boolean not) {
    this.value = value;
    this.filterType = filterType;
    this.not = not;
    try {
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
      this.pattern = Pattern.compile(patternStrBuild.toString());
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
    return not != pattern.matcher(value.toString()).find();
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
    return new Like<>(value, filterType, not);
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
  }

  @Override
  public String toString() {
    return filterType + (not ? " not like " : " like ") + value;
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.LIKE;
  }

  @Override
  public Filter reverse() {
    return new Like<>(value, filterType, !not);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Like)) {
      return false;
    }
    Like<?> like = (Like<?>) o;
    return not == like.not && value.equals(like.value) && filterType == like.filterType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, filterType, not);
  }

  /**
   * This Method is for unescaping strings except '\' before special string '%', '_', '\', because
   * we need to use '\' to judege whether to replace this to regexp string
   */
  private String unescapeString(String value) {
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
}
