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

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.SerializeUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoteTsFileResource extends TsFileResource {

  private Node source;
  private boolean isRemote = false;
  private boolean withModification = false;

  /**
   * Whether the plan range ([minPlanIndex, maxPlanIndex]) overlaps with another TsFile in the same
   * time partition. If not (unique = true), we shall have confidence that the file has all data
   * whose plan indexes are within [minPlanIndex, maxPlanIndex], so we can remove other local files
   * that overlaps with it.
   */
  private boolean isPlanRangeUnique = false;

  public RemoteTsFileResource() {
    setClosed(true);
    this.timeIndex = IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().getTimeIndex();
  }

  private RemoteTsFileResource(TsFileResource other) throws IOException {
    super(other);
    withModification = new File(getModFile().getFilePath()).exists();
    setClosed(true);
  }

  public RemoteTsFileResource(TsFileResource other, Node source) throws IOException {
    this(other);
    this.source = source;
    this.isRemote = true;
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
    NodeSerializeUtils.serialize(source, dataOutputStream);
    try {
      // the path here is only for the remote node to get a download link, so it does not matter
      // if it is absolute
      SerializeUtils.serialize(getTsFile().getPath(), dataOutputStream);

      timeIndex.serialize(dataOutputStream);
      dataOutputStream.writeBoolean(withModification);

      dataOutputStream.writeLong(maxPlanIndex);
      dataOutputStream.writeLong(minPlanIndex);

      dataOutputStream.writeByte(isPlanRangeUnique ? 1 : 0);
    } catch (IOException ignored) {
      // unreachable
    }
  }

  public void deserialize(ByteBuffer buffer) {
    source = new Node();
    NodeSerializeUtils.deserialize(source, buffer);
    setFile(new File(SerializeUtils.deserializeString(buffer)));

    timeIndex =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getTimeIndexLevel()
            .getTimeIndex()
            .deserialize(buffer);

    withModification = buffer.get() == 1;

    maxPlanIndex = buffer.getLong();
    minPlanIndex = buffer.getLong();

    isPlanRangeUnique = buffer.get() == 1;

    isRemote = true;
  }

  public Node getSource() {
    return source;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public void setRemote(boolean remote) {
    isRemote = remote;
  }

  public boolean isWithModification() {
    return withModification;
  }

  public boolean isPlanRangeUnique() {
    return isPlanRangeUnique;
  }

  public void setPlanRangeUnique(boolean planRangeUnique) {
    isPlanRangeUnique = planRangeUnique;
  }
}
