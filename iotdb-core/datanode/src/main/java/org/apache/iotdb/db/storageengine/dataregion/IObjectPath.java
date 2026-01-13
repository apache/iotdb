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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface IObjectPath {

  CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();

  int serialize(ByteBuffer byteBuffer);

  int serialize(OutputStream outputStream) throws IOException;

  int getSerializedSize();

  void serializeToObjectValue(ByteBuffer byteBuffer);

  int getSerializeSizeToObjectValue();

  interface Factory {

    IObjectPath create(int regionId, long time, IDeviceID iDeviceID, String measurement);

    Factory FACTORY = null;
  }

  interface Deserializer {

    IObjectPath deserializeFrom(ByteBuffer byteBuffer);

    IObjectPath deserializeFrom(InputStream inputStream) throws IOException;

    IObjectPath deserializeFromObjectValue(ByteBuffer byteBuffer);
  }

  static Deserializer getDeserializer() {
    return null;
  }
}
