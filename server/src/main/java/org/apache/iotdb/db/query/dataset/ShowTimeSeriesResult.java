/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.dataset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class ShowTimeSeriesResult implements Comparable<ShowTimeSeriesResult> {

  private String name;
  private String alias;
  private String sgName;
  private String dataType;
  private String encoding;
  private String compressor;
  private Map<String, String> tagAndAttribute;

  public ShowTimeSeriesResult(String name, String alias, String sgName, String dataType,
      String encoding, String compressor, Map<String, String> tagAndAttribute) {
    this.name = name;
    this.alias = alias;
    this.sgName = sgName;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tagAndAttribute = tagAndAttribute;
  }

  public ShowTimeSeriesResult() {

  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public String getSgName() {
    return sgName;
  }

  public String getDataType() {
    return dataType;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getCompressor() {
    return compressor;
  }

  public Map<String, String> getTagAndAttribute() {
    return tagAndAttribute;
  }

  @Override
  public int compareTo(ShowTimeSeriesResult o) {
    return this.name.compareTo(o.name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowTimeSeriesResult result = (ShowTimeSeriesResult) o;
    return Objects.equals(name, result.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(alias != null, outputStream); //flag
    if (alias != null) {
      ReadWriteIOUtils.write(alias, outputStream);
    }
    ReadWriteIOUtils.write(sgName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(encoding, outputStream);
    ReadWriteIOUtils.write(compressor, outputStream);

    ReadWriteIOUtils.write(tagAndAttribute != null, outputStream); //flag
    if (tagAndAttribute != null) {
      ReadWriteIOUtils.write(tagAndAttribute.size(), outputStream);
      for (Entry<String, String> stringStringEntry : tagAndAttribute.entrySet()) {
        ReadWriteIOUtils.write(stringStringEntry.getKey(), outputStream);
        ReadWriteIOUtils.write(stringStringEntry.getValue(), outputStream);
      }
    }
  }

  public static ShowTimeSeriesResult deserialize(ByteBuffer buffer) {
    ShowTimeSeriesResult result = new ShowTimeSeriesResult();
    result.name = ReadWriteIOUtils.readString(buffer);
    if (buffer.get() == 1) { //flag
      result.alias = ReadWriteIOUtils.readString(buffer);
    }
    result.sgName = ReadWriteIOUtils.readString(buffer);
    result.dataType = ReadWriteIOUtils.readString(buffer);
    result.encoding = ReadWriteIOUtils.readString(buffer);
    result.compressor = ReadWriteIOUtils.readString(buffer);

    if (buffer.get() == 1) { //flag
      int tagSize = buffer.getInt();
      result.tagAndAttribute = new HashMap<>(tagSize);
      for (int i = 0; i < tagSize; i++) {
        String key = ReadWriteIOUtils.readString(buffer);
        String value = ReadWriteIOUtils.readString(buffer);
        result.tagAndAttribute.put(key, value);
      }
    }
    return result;
  }
}