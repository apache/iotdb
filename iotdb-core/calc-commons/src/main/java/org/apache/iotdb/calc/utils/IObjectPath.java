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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import com.timecho.iotdb.calc.storageengine.dataregion.Base32ObjectPath;
import com.timecho.iotdb.calc.storageengine.dataregion.PlainObjectPath;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface IObjectPath {

  CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();

  int serialize(ByteBuffer byteBuffer);

  int serialize(OutputStream outputStream) throws IOException;

  int getSerializedSize();

  void serializeToObjectValue(ByteBuffer byteBuffer);

  int getSerializeSizeToObjectValue();

  long getTime();

  IDeviceID getDeviceID();

  String getMeasurement();

  Path getPath();

  interface Factory {

    IObjectPath create(int regionId, long time, IDeviceID iDeviceID, String measurement);

    Factory FACTORY =
        CONFIG.isRestrictObjectLimit()
            ? PlainObjectPath.getFACTORY()
            : Base32ObjectPath.getFACTORY();
  }

  interface Deserializer {

    IObjectPath deserializeFrom(ByteBuffer byteBuffer);

    IObjectPath deserializeFrom(InputStream inputStream) throws IOException;

    IObjectPath deserializeFromObjectValue(ByteBuffer byteBuffer);
  }

  static Deserializer getDeserializer() {
    return CONFIG.isRestrictObjectLimit()
        ? PlainObjectPath.getDESERIALIZER()
        : Base32ObjectPath.getDESERIALIZER();
  }

  static IObjectPath fromRelativePath(String relativePath) {
    return CONFIG.isRestrictObjectLimit()
        ? new PlainObjectPath(relativePath)
        : new Base32ObjectPath(Paths.get(relativePath));
  }

  default IObjectPath withTsFileVersion(long tsFileVersion) {
    return fromRelativePath(
        ObjectPathNaming.withTsFileVersion(toString(), getTime(), tsFileVersion));
  }

  default boolean hasTsFileVersion() {
    return ObjectPathNaming.parseVersion(getPath().getFileName().toString())
        != ObjectPathNaming.LEGACY_VERSION;
  }

  default long getTsFileVersion() {
    return ObjectPathNaming.parseVersion(getPath().getFileName().toString());
  }
}
