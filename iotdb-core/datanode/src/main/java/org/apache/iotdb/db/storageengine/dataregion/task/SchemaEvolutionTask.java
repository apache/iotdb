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

package org.apache.iotdb.db.storageengine.dataregion.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolution;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

public class SchemaEvolutionTask implements DataRegionTask {

  private List<SchemaEvolution> schemaEvolutions;
  private final DataRegion dataRegion;
  private long taskId;

  @Override
  public void run() {
    dataRegion.recordSchemaEvolution(schemaEvolutions);
    dataRegion.applySchemaEvolutionToObjects(schemaEvolutions);
  }

  public SchemaEvolutionTask(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
  }

  public SchemaEvolutionTask(List<SchemaEvolution> schemaEvolutions, DataRegion dataRegion) {
    this.schemaEvolutions = schemaEvolutions;
    this.dataRegion = dataRegion;
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    long size = ReadWriteForEncodingUtils.writeVarInt(getTaskType().ordinal(), stream);
    size += ReadWriteForEncodingUtils.writeVarInt(schemaEvolutions.size(), stream);
    for (SchemaEvolution schemaEvolution : schemaEvolutions) {
      size += schemaEvolution.serialize(stream);
    }
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    int size = ReadWriteForEncodingUtils.readVarInt(stream);
    schemaEvolutions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      schemaEvolutions.add(SchemaEvolution.createFrom(stream));
    }
  }

  @Override
  public long getTaskId() {
    return taskId;
  }

  @Override
  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.SchemaEvolutionTask;
  }
}
