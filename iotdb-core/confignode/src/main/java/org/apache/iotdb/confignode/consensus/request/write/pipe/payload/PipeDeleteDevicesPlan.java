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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PipeDeleteDevicesPlan extends AbstractTablePlan {
  private byte[] patternBytes;
  private byte[] filterBytes;
  private byte[] modBytes;

  public PipeDeleteDevicesPlan() {
    super(ConfigPhysicalPlanType.PipeDeleteDevices);
  }

  public PipeDeleteDevicesPlan(
      final String database,
      final String tableName,
      final @Nonnull byte[] patternBytes,
      final @Nonnull byte[] filterBytes,
      final @Nonnull byte[] modBytes) {
    super(ConfigPhysicalPlanType.PipeDeleteDevices, database, tableName);
    this.patternBytes = patternBytes;
    this.filterBytes = filterBytes;
    this.modBytes = modBytes;
  }

  public byte[] getPatternBytes() {
    return patternBytes;
  }

  public byte[] getFilterBytes() {
    return filterBytes;
  }

  public byte[] getModBytes() {
    return modBytes;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(patternBytes.length, stream);
    stream.write(patternBytes);
    ReadWriteIOUtils.write(filterBytes.length, stream);
    stream.write(filterBytes);
    ReadWriteIOUtils.write(modBytes.length, stream);
    stream.write(modBytes);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    patternBytes = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(patternBytes);
    filterBytes = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(filterBytes);
    modBytes = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(modBytes);
  }

  @Override
  public boolean equals(final Object obj) {
    return super.equals(obj)
        && Arrays.equals(this.patternBytes, ((PipeDeleteDevicesPlan) obj).patternBytes)
        && Arrays.equals(this.filterBytes, ((PipeDeleteDevicesPlan) obj).filterBytes)
        && Arrays.equals(this.modBytes, ((PipeDeleteDevicesPlan) obj).modBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        Arrays.hashCode(patternBytes),
        Arrays.hashCode(filterBytes),
        Arrays.hashCode(modBytes));
  }
}
