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

package org.apache.iotdb.confignode.consensus.request.write.pipe.payload;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeAlterTimeSeriesPlan extends ConfigPhysicalPlan {

  private MeasurementPath measurementPath;
  private byte operationType;
  private TSDataType dataType;

  public PipeAlterTimeSeriesPlan() {
    super(ConfigPhysicalPlanType.PipeAlterTimeSeries);
  }

  public PipeAlterTimeSeriesPlan(
      final MeasurementPath measurementPath, final byte operationType, final TSDataType dataType) {
    super(ConfigPhysicalPlanType.PipeAlterTimeSeries);
    this.measurementPath = measurementPath;
    this.operationType = operationType;
    this.dataType = dataType;
  }

  public MeasurementPath getMeasurementPath() {
    return measurementPath;
  }

  public byte getOperationType() {
    return operationType;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    measurementPath.serialize(stream);
    ReadWriteIOUtils.write(operationType, stream);
    ReadWriteIOUtils.write(dataType, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    measurementPath = (MeasurementPath) PathDeserializeUtil.deserialize(buffer);
    operationType = ReadWriteIOUtils.readByte(buffer);
    dataType = TSDataType.deserialize(ReadWriteIOUtils.readByte(buffer));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeAlterTimeSeriesPlan that = (PipeAlterTimeSeriesPlan) o;
    return this.measurementPath.equals(that.measurementPath)
        && this.operationType == that.operationType
        && this.dataType == that.dataType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(measurementPath, operationType, dataType);
  }

  @Override
  public String toString() {
    return "PipeAlterTimeSeriesPlan{"
        + "measurementPath="
        + measurementPath
        + ", operationType="
        + operationType
        + ", dataType="
        + dataType
        + '}';
  }
}
