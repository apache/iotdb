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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

// TODO: rename to PathDeviceID (countering TupleDeviceID or ArrayDeviceID)
/** Using device id path as id. */
public class PlainDeviceID implements IDeviceID {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PlainDeviceID.class)
          + RamUsageEstimator.shallowSizeOfInstance(String.class)
          + RamUsageEstimator.shallowSizeOfInstance(String.class);
  private final String deviceID;
  private String tableName;
  private String[] segments;

  public PlainDeviceID(String deviceID) {
    this.deviceID = deviceID;
  }

  private static final Deserializer DESERIALIZER =
      new Deserializer() {
        @Override
        public IDeviceID deserializeFrom(ByteBuffer byteBuffer) {
          return deserialize(byteBuffer).convertToStringArrayDeviceId();
        }

        @Override
        public IDeviceID deserializeFrom(InputStream inputStream) throws IOException {
          return deserialize(inputStream).convertToStringArrayDeviceId();
        }
      };

  public static Deserializer getDESERIALIZER() {
    return DESERIALIZER;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PlainDeviceID)) {
      return false;
    }
    PlainDeviceID that = (PlainDeviceID) o;
    return Objects.equals(deviceID, that.deviceID);
  }

  @Override
  public int hashCode() {
    return deviceID.hashCode();
  }

  public String toString() {
    return deviceID;
  }

  public String toStringID() {
    return toString();
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    return ReadWriteIOUtils.writeVar(deviceID, byteBuffer);
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    return ReadWriteIOUtils.writeVar(deviceID, outputStream);
  }

  @Override
  public byte[] getBytes() {
    return deviceID.getBytes();
  }

  @Override
  public boolean isEmpty() {
    return deviceID.isEmpty();
  }

  @Override
  public boolean isTableModel() {
    return false;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += sizeOfCharArray(deviceID.length());
    if (tableName != null) {
      size += sizeOfCharArray(tableName.length());
    }
    if (segments != null) {
      size += RamUsageEstimator.sizeOf(segments);
    }
    return size;
  }

  public static PlainDeviceID deserialize(ByteBuffer byteBuffer) {
    return new PlainDeviceID(ReadWriteIOUtils.readVarIntString(byteBuffer));
  }

  public static PlainDeviceID deserialize(InputStream inputStream) throws IOException {
    return new PlainDeviceID(ReadWriteIOUtils.readVarIntString(inputStream));
  }

  public StringArrayDeviceID convertToStringArrayDeviceId() {
    return new StringArrayDeviceID(deviceID);
  }

  @Override
  public int compareTo(IDeviceID other) {
    if (!(other instanceof PlainDeviceID)) {
      throw new IllegalArgumentException();
    }
    return deviceID.compareTo(((PlainDeviceID) other).deviceID);
  }

  @Override
  public String getTableName() {
    if (tableName != null) {
      return tableName;
    }

    int lastSeparatorPos = -1;
    int separatorNum = 0;

    for (int i = 0; i < deviceID.length(); i++) {
      if (deviceID.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        lastSeparatorPos = i;
        separatorNum++;
        if (separatorNum == TSFileConfig.DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME) {
          break;
        }
      }
    }
    if (lastSeparatorPos == -1) {
      // not find even one separator, probably during a test, use the deviceId as the tableName
      tableName = deviceID;
    } else {
      // use the first DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME segments or all segments but the last
      // one as the table name
      tableName = deviceID.substring(0, lastSeparatorPos);
    }

    return tableName;
  }

  @Override
  public int segmentNum() {
    if (segments != null) {
      return segments.length;
    }
    segments = deviceID.split(TsFileConstant.PATH_SEPARATER_NO_REGEX);
    return segments.length;
  }

  @Override
  public String segment(int i) {
    if (i >= segmentNum()) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return segments[i];
  }

  public static class Factory implements IDeviceID.Factory {

    @Override
    public IDeviceID create(String deviceIdString) {
      return new PlainDeviceID(deviceIdString);
    }

    @Override
    public IDeviceID create(String[] segments) {
      return new PlainDeviceID(String.join(TsFileConstant.PATH_SEPARATOR, segments));
    }
  }
}
