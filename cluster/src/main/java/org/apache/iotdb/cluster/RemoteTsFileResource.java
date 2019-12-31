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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class RemoteTsFileResource extends TsFileResource {

  private Node source;
  //TODO why it is false by default?
  private boolean isRemote = false;
  private byte[] md5;
  private boolean withModification = false;

  public RemoteTsFileResource() {
    setClosed(true);
  }

  private RemoteTsFileResource(TsFileResource other) {
    super(other);
    md5 = getFileMd5(other);
    withModification = new File(getModFile().getFilePath()).exists();
    setClosed(true);
  }

  public RemoteTsFileResource(TsFileResource other, Node source) {
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
      SerializeUtils.serialize(getFile().getAbsolutePath(), dataOutputStream);
      Map<String, Long> startTimeMap = getStartTimeMap();
      Map<String, Long> endTimeMap = getEndTimeMap();
      dataOutputStream.writeInt(startTimeMap.size());
      for (Entry<String, Long> entry : startTimeMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.writeLong(entry.getValue());
      }
      dataOutputStream.writeInt(endTimeMap.size());
      for (Entry<String, Long> entry : endTimeMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.writeLong(entry.getValue());
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

    Map<String, Long> startTimeMap = new HashMap<>();
    Map<String, Long> endTimeMap = new HashMap<>();
    int startMapSize = buffer.getInt();
    for (int i = 0; i < startMapSize; i++) {
      startTimeMap.put(SerializeUtils.deserializeString(buffer), buffer.getLong());
    }
    int endMapSize = buffer.getInt();
    for (int i = 0; i < endMapSize; i++) {
      endTimeMap.put(SerializeUtils.deserializeString(buffer), buffer.getLong());
    }

    withModification = buffer.get() == 1;

    setStartTimeMap(startTimeMap);
    setEndTimeMap(endTimeMap);

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
