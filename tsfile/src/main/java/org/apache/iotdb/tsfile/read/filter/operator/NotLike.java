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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.iotdb.tsfile.read.filter.operator.Like.unescapeString;

public class NotLike<T extends Comparable<T>> implements Filter, Serializable {

  protected String value;

  protected FilterType filterType;

  protected Pattern pattern;

  private NotLike() {}

  public NotLike(String value, FilterType filterType) {
    this.value = value;
    this.filterType = filterType;
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
      throw new PatternSyntaxException("Regular expression error", value.toString(), e.getIndex());
    }
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    if (filterType != FilterType.VALUE_FILTER) {
      throw new UnsupportedOperationException("");
    }
    return true;
  }

  @Override
  public boolean allSatisfy(Statistics statistics) {
    if (filterType != FilterType.VALUE_FILTER) {
      throw new UnsupportedOperationException("");
    }
    return false;
  }

  @Override
  public boolean satisfy(long time, Object value) {
    if (filterType != FilterType.VALUE_FILTER) {
      throw new UnsupportedOperationException("");
    }
    return !pattern.matcher(value.toString()).find();
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Filter copy() {
    return new NotLike(value, filterType);
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.writeObject(value, outputStream);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Failed to serialize outputStream of type:", ex);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    value = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public String toString() {
    return filterType + " not like " + value;
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.NOT_LIKE;
  }
}
