/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * FileSnapshot records the data files in a slot and their md5 (or other verification).
 * When the snapshot is used to perform a catch-up, the receiver should:
 * 1. create a remote snapshot indicating that the slot is being pulled from the remote
 * 2. traverse the file list, for each file:
 *  2.1 if the file exists locally and the md5 is correct, skip it.
 *  2.2 otherwise pull the file from the remote.
 * 3. replace the remote snapshot with a FileSnapshot indicating that the slot of this node is
 * synchronized with the remote one.
 */
public class FileSnapshot extends Snapshot implements TimeseriesSchemaSnapshot {

  protected Set<MeasurementSchema> timeseriesSchemas;
  protected List<RemoteTsFileResource> dataFiles;

  public FileSnapshot() {
    dataFiles = new ArrayList<>();
    timeseriesSchemas = new HashSet<>();
  }

  public void addFile(TsFileResource resource, Node header) {
    dataFiles.add(new RemoteTsFileResource(resource, header));
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (MeasurementSchema measurementSchema : timeseriesSchemas) {
        measurementSchema.serializeTo(dataOutputStream);
      }
      dataOutputStream.writeInt(dataFiles.size());
      for (RemoteTsFileResource dataFile : dataFiles) {
        dataFile.serialize(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int timeseriesNum = buffer.getInt();
    for (int i = 0; i < timeseriesNum; i++) {
      timeseriesSchemas.add(MeasurementSchema.deserializeFrom(buffer));
    }
    int fileNum = buffer.getInt();
    for (int i = 0; i < fileNum; i++) {
      RemoteTsFileResource resource = new RemoteTsFileResource();
      resource.deserialize(buffer);
      dataFiles.add(resource);
    }
  }

  public List<RemoteTsFileResource> getDataFiles() {
    return dataFiles;
  }

  @Override
  public Set<MeasurementSchema> getTimeseriesSchemas() {
    return timeseriesSchemas;
  }

  @Override
  public void setTimeseriesSchemas(
      Collection<MeasurementSchema> timeseriesSchemas) {
    this.timeseriesSchemas = (Set) timeseriesSchemas;
  }

  @Override
  public String toString() {
    return "FileSnapshot{" +
        "timeseriesSchemas=" + timeseriesSchemas.size() +
        ", dataFiles=" + dataFiles.size() +
        ", lastLogId=" + lastLogId +
        ", lastLogTerm=" + lastLogTerm +
        '}';
  }
}
