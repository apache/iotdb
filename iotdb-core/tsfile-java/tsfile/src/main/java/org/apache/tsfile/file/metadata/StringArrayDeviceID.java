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
import org.apache.tsfile.exception.IllegalDeviceIDException;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.read.common.parser.PathNodesGenerator;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.WriteUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class StringArrayDeviceID implements IDeviceID {

  private int serializedSize = -1;

  private static final Deserializer DESERIALIZER =
      new Deserializer() {
        @Override
        public IDeviceID deserializeFrom(ByteBuffer byteBuffer) {
          return deserialize(byteBuffer);
        }

        @Override
        public IDeviceID deserializeFrom(InputStream inputStream) throws IOException {
          return deserialize(inputStream);
        }
      };

  private static final Factory FACTORY =
      new Factory() {
        @Override
        public IDeviceID create(String deviceIdString) {
          return new StringArrayDeviceID(deviceIdString);
        }

        @Override
        public IDeviceID create(String[] segments) {
          return new StringArrayDeviceID(segments);
        }
      };

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StringArrayDeviceID.class);

  // TODO: change to Object[] and rename to just ArrayDeviceID
  // or we can just use a tuple like Relational DB.
  private final String[] segments;

  public StringArrayDeviceID(String... segments) {
    this.segments = formalize(segments);
  }

  public StringArrayDeviceID(String deviceIdString) {
    this.segments = splitDeviceIdString(deviceIdString);
  }

  private String[] formalize(String[] segments) {
    // remove tailing nulls
    int i = segments.length - 1;
    for (; i >= 0; i--) {
      if (segments[i] != null) {
        break;
      }
    }
    if (i < 0) {
      throw new IllegalDeviceIDException("All segments are null");
    }
    if (i != segments.length - 1) {
      segments = Arrays.copyOf(segments, i + 1);
    }
    return segments;
  }

  @SuppressWarnings("java:S125") // confusing comments with codes
  public static String[] splitDeviceIdString(String deviceIdString) {
    return splitDeviceIdString(PathNodesGenerator.splitPathToNodes(deviceIdString));
  }

  @SuppressWarnings("java:S125") // confusing comments with codes
  public static String[] splitDeviceIdString(String[] splits) {
    int segmentCnt = splits.length;

    String tableName;
    String[] finalSegments;
    // assuming DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME = 3
    if (segmentCnt == 1) {
      // "root" -> {"root"}
      finalSegments = new String[1];
      finalSegments[0] = splits[0];
    } else if (segmentCnt < TSFileConfig.DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME + 1) {
      // "root.a" -> {"root", "a"}
      // "root.a.b" -> {"root.a", "b"}
      tableName = String.join(PATH_SEPARATOR, Arrays.copyOfRange(splits, 0, segmentCnt - 1));
      finalSegments = new String[2];
      finalSegments[0] = tableName;
      finalSegments[1] = splits[segmentCnt - 1];
    } else {
      // "root.a.b.c" -> {"root.a.b", "c"}
      // "root.a.b.c.d" -> {"root.a.b", "c", "d"}
      tableName =
          String.join(
              PATH_SEPARATOR,
              Arrays.copyOfRange(splits, 0, TSFileConfig.DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME));

      String[] idSegments =
          Arrays.copyOfRange(splits, TSFileConfig.DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME, segmentCnt);
      finalSegments = new String[idSegments.length + 1];
      finalSegments[0] = tableName;
      System.arraycopy(idSegments, 0, finalSegments, 1, idSegments.length);
    }

    return finalSegments;
  }

  public static Deserializer getDESERIALIZER() {
    return DESERIALIZER;
  }

  public static Factory getFACTORY() {
    return FACTORY;
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    int cnt = 0;
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(segments.length, byteBuffer);
    for (String segment : segments) {
      cnt += ReadWriteIOUtils.writeVar(segment, byteBuffer);
    }
    return cnt;
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    int cnt = 0;
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(segments.length, outputStream);
    for (String segment : segments) {
      cnt += ReadWriteIOUtils.writeVar(segment, outputStream);
    }
    return cnt;
  }

  public static StringArrayDeviceID deserialize(ByteBuffer byteBuffer) {
    final int cnt = ReadWriteForEncodingUtils.readUnsignedVarInt(byteBuffer);
    if (cnt == 0) {
      return new StringArrayDeviceID(new String[] {""});
    }

    String[] segments = new String[cnt];
    for (int i = 0; i < cnt; i++) {
      segments[i] = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    return new StringArrayDeviceID(segments);
  }

  public static StringArrayDeviceID deserialize(InputStream stream) throws IOException {
    final int cnt = ReadWriteForEncodingUtils.readUnsignedVarInt(stream);
    if (cnt == 0) {
      return new StringArrayDeviceID(new String[] {""});
    }

    String[] segments = new String[cnt];
    for (int i = 0; i < cnt; i++) {
      segments[i] = ReadWriteIOUtils.readVarIntString(stream);
    }
    return new StringArrayDeviceID(segments);
  }

  @Override
  public byte[] getBytes() {
    ByteArrayOutputStream publicBAOS = new ByteArrayOutputStream(256);
    try {
      serialize(publicBAOS);
    } catch (IOException e) {
      throw new TsFileRuntimeException(e);
    }
    return publicBAOS.toByteArray();
  }

  @Override
  public boolean isEmpty() {
    return segments == null || segments.length == 0;
  }

  @Override
  public boolean isTableModel() {
    return !segments[0].startsWith(PATH_ROOT + PATH_SEPARATOR);
  }

  @Override
  public String getTableName() {
    return segments[0];
  }

  @Override
  public int segmentNum() {
    return segments.length;
  }

  @Override
  public String segment(int i) {
    if (i >= segments.length) {
      // after removing trailing nulls, the provided index may be larger than the number of segments
      return null;
    }
    return segments[i];
  }

  @Override
  public int compareTo(IDeviceID o) {
    int thisSegmentNum = segmentNum();
    int otherSegmentNum = o.segmentNum();
    for (int i = 0; i < thisSegmentNum; i++) {
      if (i >= otherSegmentNum) {
        // the other ID is a prefix of this one
        return 1;
      }
      final int comp =
          Objects.compare(this.segment(i), ((String) o.segment(i)), WriteUtils::compareStrings);
      if (comp != 0) {
        // the partial comparison has a result
        return comp;
      }
    }

    if (thisSegmentNum < otherSegmentNum) {
      // this ID is a prefix of the other one
      return -1;
    }

    // two ID equal
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + RamUsageEstimator.sizeOf(segments);
  }

  @Override
  public int serializedSize() {
    if (serializedSize != -1) {
      return serializedSize;
    }

    int cnt = ReadWriteForEncodingUtils.varIntSize(segments.length);
    for (String segment : segments) {
      if (segment != null) {
        byte[] bytes = segment.getBytes(TSFileConfig.STRING_CHARSET);
        cnt += ReadWriteForEncodingUtils.varIntSize(bytes.length);
        cnt += bytes.length;
      } else {
        cnt += ReadWriteForEncodingUtils.varIntSize(ReadWriteIOUtils.NO_BYTE_TO_READ);
      }
    }
    serializedSize = cnt;
    return cnt;
  }

  @Override
  public String toString() {
    return String.join(".", segments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringArrayDeviceID deviceID = (StringArrayDeviceID) o;
    return Objects.deepEquals(segments, deviceID.segments);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(segments);
  }

  @Override
  public String[] getSegments() {
    return segments;
  }
}
