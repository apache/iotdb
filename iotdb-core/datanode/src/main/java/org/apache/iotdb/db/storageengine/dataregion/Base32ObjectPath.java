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

package org.apache.iotdb.db.storageengine.dataregion;

import com.google.common.io.BaseEncoding;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Base32ObjectPath implements IObjectPath {

  private final Path path;
  private int serializedSize = -1;

  private static final Deserializer DESERIALIZER =
      new Deserializer() {
        @Override
        public IObjectPath deserializeFrom(ByteBuffer byteBuffer) {
          return deserialize(byteBuffer);
        }

        @Override
        public IObjectPath deserializeFrom(InputStream inputStream) throws IOException {
          return deserialize(inputStream);
        }

        @Override
        public IObjectPath deserializeFromObjectValue(ByteBuffer byteBuffer) {
          return deserialize(byteBuffer);
        }
      };

  private static final Factory FACTORY = Base32ObjectPath::new;

  private Base32ObjectPath(String first, String... more) {
    path = Paths.get(first, more);
  }

  public Base32ObjectPath(Path path) {
    this.path = path;
  }

  public Base32ObjectPath(int regionId, long time, IDeviceID iDeviceID, String measurement) {
    Object[] segments = iDeviceID.getSegments();
    String[] pathSegments = new String[segments.length + 2];
    for (int i = 0; i < segments.length; i++) {
      Object segment = segments[i];
      String segmentString = segment == null ? "null" : segment.toString();
      pathSegments[i] =
          BaseEncoding.base32()
              .omitPadding()
              .encode(segmentString.getBytes(StandardCharsets.UTF_8));
    }
    pathSegments[pathSegments.length - 2] =
        BaseEncoding.base32().omitPadding().encode(measurement.getBytes(StandardCharsets.UTF_8));
    pathSegments[pathSegments.length - 1] = time + ".bin";
    path = Paths.get(String.valueOf(regionId), pathSegments);
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    int cnt = 0;
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(path.getNameCount(), byteBuffer);
    for (Path segment : path) {
      cnt += ReadWriteIOUtils.writeVar(segment.toString(), byteBuffer);
    }
    return cnt;
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    int cnt = 0;
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(path.getNameCount(), outputStream);
    for (Path segment : path) {
      cnt += ReadWriteIOUtils.writeVar(segment.toString(), outputStream);
    }
    return cnt;
  }

  @Override
  public int getSerializedSize() {
    if (serializedSize != -1) {
      return serializedSize;
    }
    int cnt = ReadWriteForEncodingUtils.varIntSize(path.getNameCount());
    for (Path segment : path) {
      byte[] bytes = segment.toString().getBytes(StandardCharsets.UTF_8);
      cnt += ReadWriteForEncodingUtils.varIntSize(bytes.length);
      cnt += bytes.length;
    }
    serializedSize = cnt;
    return cnt;
  }

  @Override
  public void serializeToObjectValue(ByteBuffer byteBuffer) {
    serialize(byteBuffer);
  }

  @Override
  public int getSerializeSizeToObjectValue() {
    return getSerializedSize();
  }

  public static Base32ObjectPath deserialize(ByteBuffer byteBuffer) {
    int cnt = ReadWriteForEncodingUtils.readUnsignedVarInt(byteBuffer);
    String first = ReadWriteIOUtils.readVarIntString(byteBuffer);
    String[] more = new String[cnt - 1];

    for (int i = 0; i < cnt - 1; ++i) {
      more[i] = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    return new Base32ObjectPath(first, more);
  }

  public static Base32ObjectPath deserialize(InputStream stream) throws IOException {
    int cnt = ReadWriteForEncodingUtils.readUnsignedVarInt(stream);
    String first = ReadWriteIOUtils.readVarIntString(stream);
    String[] more = new String[cnt - 1];

    for (int i = 0; i < cnt - 1; ++i) {
      more[i] = ReadWriteIOUtils.readVarIntString(stream);
    }

    return new Base32ObjectPath(first, more);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  public Path getPath() {
    return path;
  }

  public static Factory getFACTORY() {
    return FACTORY;
  }

  public static Deserializer getDESERIALIZER() {
    return DESERIALIZER;
  }
}
