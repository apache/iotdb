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

package org.apache.iotdb.cluster;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.SerializeUtils;

public class RemoteTsFileResource extends TsFileResource {

  private Node source;
  private boolean isRemote = false;
  private byte[] md5;
  private boolean withModification = false;

  public RemoteTsFileResource() {
    setClosed(true);
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  private RemoteTsFileResource(TsFileResource other) throws IOException {
    super(other);
    md5 = getFileMd5(other);
    withModification = new File(getModFile().getFilePath()).exists();
    setClosed(true);
  }

  public RemoteTsFileResource(TsFileResource other, Node source) throws IOException {
    this(other);
    this.source = source;
    this.isRemote = true;
  }

  private byte[] getFileMd5(TsFileResource resource) {
    // TODO-Cluster#353: implement
    return new byte[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RemoteTsFileResource that = (RemoteTsFileResource) o;
    return Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), source);
  }

  public void serialize(DataOutputStream dataOutputStream) {
    SerializeUtils.serialize(source, dataOutputStream);
    try {
      dataOutputStream.writeInt(md5.length);
      dataOutputStream.write(md5);
      // the path here is only for the remote node to get a download link, so it does not matter
      // if it is absolute
      SerializeUtils.serialize(getFile().getPath(), dataOutputStream);

      int deviceNum = startTimes.length;
      dataOutputStream.writeInt(startTimes.length);
      for (int i = 0; i < deviceNum; i++) {
        dataOutputStream.writeLong(startTimes[i]);
        dataOutputStream.writeLong(endTimes[i]);
      }

      for (Entry<String, Integer> stringIntegerEntry : deviceToIndex.entrySet()) {
        String deviceName = stringIntegerEntry.getKey();
        int index = stringIntegerEntry.getValue();
        SerializeUtils.serialize(deviceName, dataOutputStream);
        dataOutputStream.writeInt(index);
      }

      dataOutputStream.writeBoolean(withModification);

      dataOutputStream.writeInt(getHistoricalVersions().size());
      for (long hisVersion : getHistoricalVersions()) {
        dataOutputStream.writeLong(hisVersion);
      }
    } catch (IOException ignored) {
      // unreachable
    }
  }

  public void deserialize(ByteBuffer buffer) {
    source = new Node();
    SerializeUtils.deserialize(source, buffer);
    int md5Length = buffer.getInt();
    md5 = new byte[md5Length];
    buffer.get(md5);
    setFile(new File(SerializeUtils.deserializeString(buffer)));

    int deviceNum = buffer.getInt();
    startTimes = new long[deviceNum];
    endTimes = new long[deviceNum];
    deviceToIndex = new ConcurrentHashMap<>(deviceNum);

    for (int i = 0; i < deviceNum; i++) {
      startTimes[i] = buffer.getLong();
      endTimes[i] = buffer.getLong();
    }

    for (int i = 0; i < deviceNum; i++) {
      String deviceName = SerializeUtils.deserializeString(buffer);
      int index = buffer.getInt();
      deviceToIndex.put(deviceName, index);
    }

    withModification = buffer.get() == 1;

    Set<Long> historicalVersions = new HashSet<>();
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      historicalVersions.add(buffer.getLong());
    }
    setHistoricalVersions(historicalVersions);

    isRemote = true;
  }

  public Node getSource() {
    return source;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public byte[] getMd5() {
    return md5;
  }

  public void setRemote(boolean remote) {
    isRemote = remote;
  }

  public boolean isWithModification() {
    return withModification;
  }

}
