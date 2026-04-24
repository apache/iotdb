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

package com.timecho.iotdb.calc.storageengine.dataregion;

import org.apache.iotdb.calc.utils.IObjectPath;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PlainObjectPath implements IObjectPath {

  private final IDeviceID deviceID;
  private final long timestamp;
  private final String measurement;
  private final String filePath;

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
          return deserializeObjectValue(byteBuffer);
        }
      };

  private static final Factory FACTORY = PlainObjectPath::new;

  public PlainObjectPath(String filePath) {
    this.filePath = filePath;
    Path path = Paths.get(filePath);
    String[] ideviceIdSegments = new String[path.getNameCount() - 3];
    for (int i = 0; i < ideviceIdSegments.length; i++) {
      ideviceIdSegments[i] = path.getName(i + 1).toString();
      if ("NUL".equals(ideviceIdSegments[i])) {
        ideviceIdSegments[i] = null;
      } else if ("EPT".equals(ideviceIdSegments[i])) {
        ideviceIdSegments[i] = "";
      }
    }
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(ideviceIdSegments);
    measurement = path.getName(path.getNameCount() - 2).toString();
    String fileName = path.getFileName().toString();
    timestamp = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
  }

  public PlainObjectPath(Path path) {
    this.filePath = path.toString();
    String[] ideviceIdSegments = new String[path.getNameCount() - 3];
    for (int i = 0; i < ideviceIdSegments.length; i++) {
      ideviceIdSegments[i] = path.getName(i + 1).toString();
      if ("NUL".equals(ideviceIdSegments[i])) {
        ideviceIdSegments[i] = null;
      } else if ("EPT".equals(ideviceIdSegments[i])) {
        ideviceIdSegments[i] = "";
      }
    }
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(ideviceIdSegments);
    measurement = path.getName(path.getNameCount() - 2).toString();
    String fileName = path.getFileName().toString();
    timestamp = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
  }

  public PlainObjectPath(int regionId, long time, IDeviceID iDeviceID, String measurement) {
    String objectFileName = time + ".bin";
    Object[] segments = iDeviceID.getSegments();
    StringBuilder relativePathString =
        new StringBuilder(String.valueOf(regionId)).append(File.separator);
    for (Object segment : segments) {
      if (segment == null) {
        relativePathString.append("NUL");
      } else if ("".equals(segment)) {
        relativePathString.append("EPT");
      } else {
        relativePathString.append(segment.toString().toLowerCase());
      }
      relativePathString.append(File.separator);
    }
    relativePathString.append(measurement).append(File.separator);
    relativePathString.append(objectFileName);
    this.deviceID = iDeviceID;
    this.timestamp = time;
    this.measurement = measurement;
    this.filePath = relativePathString.toString();
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    return ReadWriteIOUtils.write(filePath, byteBuffer);
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    return ReadWriteIOUtils.write(filePath, outputStream);
  }

  @Override
  public int getSerializedSize() {
    return ReadWriteIOUtils.sizeToWrite(filePath);
  }

  @Override
  public void serializeToObjectValue(ByteBuffer byteBuffer) {
    byteBuffer.put(filePath.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public int getSerializeSizeToObjectValue() {
    return filePath.getBytes(StandardCharsets.UTF_8).length;
  }

  public static PlainObjectPath deserialize(ByteBuffer byteBuffer) {
    String filePath = ReadWriteIOUtils.readString(byteBuffer);
    return new PlainObjectPath(filePath);
  }

  public static PlainObjectPath deserialize(InputStream stream) throws IOException {
    String filePath = ReadWriteIOUtils.readString(stream);
    return new PlainObjectPath(filePath);
  }

  public static PlainObjectPath deserializeObjectValue(ByteBuffer byteBuffer) {
    return new PlainObjectPath(StandardCharsets.UTF_8.decode(byteBuffer).toString());
  }

  @Override
  public String toString() {
    return filePath;
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
    return Paths.get(filePath);
  }

  public static Factory getFACTORY() {
    return FACTORY;
  }

  public static Deserializer getDESERIALIZER() {
    return DESERIALIZER;
  }
}
