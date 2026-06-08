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

package org.apache.iotdb.calc.utils;

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

  private final long timestamp;
  private final IDeviceID deviceID;
  private final String measurement;
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
    final String[] deviceIdSegments = new String[more.length - 2];
    for (int i = 0; i < more.length - 2; i++) {
      if ("NUL".equals(more[i])) {
        deviceIdSegments[i] = null;
      } else if ("EPT".equals(more[i])) {
        deviceIdSegments[i] = "";
      } else {
        deviceIdSegments[i] =
            new String(BaseEncoding.base32().omitPadding().decode(more[i]), StandardCharsets.UTF_8);
      }
    }
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    measurement =
        new String(
            BaseEncoding.base32().omitPadding().decode(more[more.length - 2]),
            StandardCharsets.UTF_8);
    timestamp =
        Long.parseLong(more[more.length - 1].substring(0, more[more.length - 1].indexOf('.')));
    path = Paths.get(first, more);
  }

  public Base32ObjectPath(Path path) {
    final String[] deviceIdSegments = new String[path.getNameCount() - 3];
    for (int i = 0; i < deviceIdSegments.length; i++) {
      final String segment = path.getName(i + 1).toString();
      if ("NUL".equals(segment)) {
        deviceIdSegments[i] = null;
      } else if ("EPT".equals(segment)) {
        deviceIdSegments[i] = "";
      } else {
        deviceIdSegments[i] =
            new String(BaseEncoding.base32().omitPadding().decode(segment), StandardCharsets.UTF_8);
      }
    }
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    measurement =
        new String(
            BaseEncoding.base32()
                .omitPadding()
                .decode(path.getName(path.getNameCount() - 2).toString()),
            StandardCharsets.UTF_8);
    final String fileName = path.getFileName().toString();
    timestamp = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    this.path = path;
  }

  public Base32ObjectPath(int regionId, long time, IDeviceID deviceID, String measurement) {
    final Object[] segments = deviceID.getSegments();
    final String[] pathSegments = new String[segments.length + 2];
    for (int i = 0; i < segments.length; i++) {
      final Object segment = segments[i];
      if (segment == null) {
        pathSegments[i] = "NUL";
      } else if ("".equals(segment)) {
        pathSegments[i] = "EPT";
      } else {
        pathSegments[i] =
            BaseEncoding.base32()
                .omitPadding()
                .encode(segment.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
    pathSegments[pathSegments.length - 2] =
        BaseEncoding.base32().omitPadding().encode(measurement.getBytes(StandardCharsets.UTF_8));
    pathSegments[pathSegments.length - 1] = time + ".bin";
    this.path = Paths.get(String.valueOf(regionId), pathSegments);
    this.timestamp = time;
    this.deviceID = deviceID;
    this.measurement = measurement;
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    int count = 0;
    count += ReadWriteForEncodingUtils.writeUnsignedVarInt(path.getNameCount(), byteBuffer);
    for (final Path segment : path) {
      count += ReadWriteIOUtils.writeVar(segment.toString(), byteBuffer);
    }
    return count;
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    int count = 0;
    count += ReadWriteForEncodingUtils.writeUnsignedVarInt(path.getNameCount(), outputStream);
    for (final Path segment : path) {
      count += ReadWriteIOUtils.writeVar(segment.toString(), outputStream);
    }
    return count;
  }

  @Override
  public int getSerializedSize() {
    if (serializedSize != -1) {
      return serializedSize;
    }
    int count = ReadWriteForEncodingUtils.varIntSize(path.getNameCount());
    for (final Path segment : path) {
      final byte[] bytes = segment.toString().getBytes(StandardCharsets.UTF_8);
      count += ReadWriteForEncodingUtils.varIntSize(bytes.length);
      count += bytes.length;
    }
    serializedSize = count;
    return count;
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
    final int count = ReadWriteForEncodingUtils.readUnsignedVarInt(byteBuffer);
    final String first = ReadWriteIOUtils.readVarIntString(byteBuffer);
    final String[] more = new String[count - 1];
    for (int i = 0; i < count - 1; ++i) {
      more[i] = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    return new Base32ObjectPath(first, more);
  }

  public static Base32ObjectPath deserialize(InputStream stream) throws IOException {
    final int count = ReadWriteForEncodingUtils.readUnsignedVarInt(stream);
    final String first = ReadWriteIOUtils.readVarIntString(stream);
    final String[] more = new String[count - 1];
    for (int i = 0; i < count - 1; ++i) {
      more[i] = ReadWriteIOUtils.readVarIntString(stream);
    }
    return new Base32ObjectPath(first, more);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  @Override
  public long getTime() {
    return timestamp;
  }

  @Override
  public IDeviceID getDeviceID() {
    return deviceID;
  }

  @Override
  public String getMeasurement() {
    return measurement;
  }

  @Override
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
