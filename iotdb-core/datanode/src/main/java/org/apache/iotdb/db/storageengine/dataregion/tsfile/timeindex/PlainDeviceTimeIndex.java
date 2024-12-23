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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class PlainDeviceTimeIndex extends ArrayDeviceTimeIndex implements ITimeIndex {

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlainDeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);

    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      endTimes[i] = ReadWriteIOUtils.readLong(inputStream);
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      int index = ReadWriteIOUtils.readInt(inputStream);
      try {
        PartialPath partialPath = DataNodeDevicePathCache.getInstance().getPartialPath(path);
        deviceToIndex.put(partialPath.getIDeviceID(), index);
      } catch (IllegalPathException e) {
        deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create(path), index);
      }
    }
    return this;
  }

  @Override
  public PlainDeviceTimeIndex deserialize(ByteBuffer buffer) {
    int deviceNum = buffer.getInt();
    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = buffer.getLong();
      endTimes[i] = buffer.getLong();
      minStartTime = Math.min(minStartTime, startTimes[i]);
      maxEndTime = Math.max(maxEndTime, endTimes[i]);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = ReadWriteIOUtils.readString(buffer);
      int index = buffer.getInt();
      try {
        PartialPath partialPath = DataNodeDevicePathCache.getInstance().getPartialPath(path);
        deviceToIndex.put(partialPath.getIDeviceID(), index);
      } catch (IllegalPathException e) {
        deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create(path), index);
      }
    }
    return this;
  }

  /**
   * Deserialize TimeIndex and get devices only.
   *
   * @param inputStream inputStream
   * @return device name
   */
  public static Set<IDeviceID> getDevices(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);
    ReadWriteIOUtils.skip(inputStream, 2L * deviceNum * ReadWriteIOUtils.LONG_LEN);
    Set<IDeviceID> devices = new HashSet<>();
    for (int i = 0; i < deviceNum; i++) {
      String path =
          DataNodeDevicePathCache.getInstance()
              .getDeviceId(ReadWriteIOUtils.readString(inputStream));
      ReadWriteIOUtils.skip(inputStream, ReadWriteIOUtils.INT_LEN);
      devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(path));
    }
    return devices;
  }

  @Override
  public byte getTimeIndexType() {
    return PLAIN_DEVICE_TIME_INDEX_TYPE;
  }
}
