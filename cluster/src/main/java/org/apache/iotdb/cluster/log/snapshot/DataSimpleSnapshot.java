/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * DataSimpleSnapshot also records timeseries in the slot.
 */
public class DataSimpleSnapshot extends SimpleSnapshot {

  Collection<MeasurementSchema> timeseriesSchemas;

  public DataSimpleSnapshot() {
    snapshot = new ArrayList<>();
    timeseriesSchemas = new ArrayList<>();
  }

  public DataSimpleSnapshot(Collection<MeasurementSchema> timeseriesSchemas) {
    this.snapshot = new ArrayList<>();
    this.timeseriesSchemas = timeseriesSchemas;
  }

  @Override
  void subSerialize(DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (MeasurementSchema measurementSchema : timeseriesSchemas) {
        measurementSchema.serializeTo(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable
    }
  }

  @Override
  void subDeserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      timeseriesSchemas.add(MeasurementSchema.deserializeFrom(buffer));
    }
  }

  public Collection<MeasurementSchema> getTimeseriesSchemas() {
    return timeseriesSchemas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSimpleSnapshot that = (DataSimpleSnapshot) o;
    return Objects.equals(timeseriesSchemas, that.timeseriesSchemas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeseriesSchemas);
  }
}
