/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.ID;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** A schema evolution operation that renames a table in a schema map. */
public class TableRename implements SchemaEvolution {

  private String nameBefore;
  private String nameAfter;

  // for deserialization
  public TableRename() {}

  public TableRename(String nameBefore, String nameAfter) {
    this.nameBefore = nameBefore.toLowerCase();
    this.nameAfter = nameAfter.toLowerCase();
  }

  @Override
  public void applyTo(EvolvedSchema evolvedSchema) {
    evolvedSchema.renameTable(nameBefore, nameAfter);
  }

  @Override
  public SchemaEvolutionType getEvolutionType() {
    return SchemaEvolutionType.TABLE_RENAME;
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    long size = ReadWriteForEncodingUtils.writeVarInt(getEvolutionType().ordinal(), stream);
    size += ReadWriteIOUtils.writeVar(nameBefore, stream);
    size += ReadWriteIOUtils.writeVar(nameAfter, stream);
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    nameBefore = ReadWriteIOUtils.readVarIntString(stream);
    nameAfter = ReadWriteIOUtils.readVarIntString(stream);
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    long size = ReadWriteForEncodingUtils.writeVarInt(getEvolutionType().ordinal(), buffer);
    size += ReadWriteIOUtils.writeVar(nameBefore, buffer);
    size += ReadWriteIOUtils.writeVar(nameAfter, buffer);
    return size;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    nameBefore = ReadWriteIOUtils.readVarIntString(buffer);
    nameAfter = ReadWriteIOUtils.readVarIntString(buffer);
  }

  public String getNameBefore() {
    return nameBefore;
  }

  public String getNameAfter() {
    return nameAfter;
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  public IDeviceID rewriteDeviceId(IDeviceID deviceId) {
    if (!deviceId.getTableName().equals(nameBefore)) {
      return deviceId;
    }

    Object[] segments = deviceId.getSegments();
    String[] newSegments = new String[segments.length];
    newSegments[0] = nameAfter;
    System.arraycopy(segments, 1, newSegments, 1, segments.length - 1);
    return Factory.DEFAULT_FACTORY.create(newSegments);
  }

  public <T> void rewriteMap(Map<IDeviceID, T> map) {
    List<IDeviceID> affectedDeviceId = map.keySet().stream()
        .filter(k -> k.getTableName().equals(getNameBefore())).collect(
            Collectors.toList());
    for (IDeviceID deviceID : affectedDeviceId) {
      IDeviceID newDeviceId = rewriteDeviceId(deviceID);
      T removed = map.remove(deviceID);
      map.put(newDeviceId, removed);
    }
  }
}
