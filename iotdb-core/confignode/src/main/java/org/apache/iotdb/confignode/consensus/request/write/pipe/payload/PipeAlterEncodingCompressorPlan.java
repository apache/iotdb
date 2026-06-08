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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeAlterEncodingCompressorPlan extends ConfigPhysicalPlan {

  private ByteBuffer patternTreeBytes;
  private byte encoding;
  private byte compressor;
  private boolean mayAlterAudit;

  public PipeAlterEncodingCompressorPlan() {
    super(ConfigPhysicalPlanType.PipeAlterEncodingCompressor);
  }

  public PipeAlterEncodingCompressorPlan(
      final @Nonnull ByteBuffer patternTreeBytes,
      final byte encoding,
      final byte compressor,
      final boolean mayAlterAudit) {
    super(ConfigPhysicalPlanType.PipeAlterEncodingCompressor);
    this.patternTreeBytes = patternTreeBytes;
    this.encoding = encoding;
    this.compressor = compressor;
    this.mayAlterAudit = mayAlterAudit;
  }

  public void setPatternTreeBytes(ByteBuffer patternTreeBytes) {
    this.patternTreeBytes = patternTreeBytes;
  }

  public ByteBuffer getPatternTreeBytes() {
    patternTreeBytes.rewind();
    return patternTreeBytes;
  }

  public byte getEncoding() {
    return encoding;
  }

  public byte getCompressor() {
    return compressor;
  }

  public void setMayAlterAudit(final boolean mayAlterAudit) {
    this.mayAlterAudit = mayAlterAudit;
  }

  public boolean isMayAlterAudit() {
    return mayAlterAudit;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(patternTreeBytes, stream);
    ReadWriteIOUtils.write(encoding, stream);
    ReadWriteIOUtils.write(compressor, stream);
    ReadWriteIOUtils.write(mayAlterAudit, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    patternTreeBytes = ByteBuffer.wrap(ReadWriteIOUtils.readBinary(buffer).getValues());
    encoding = ReadWriteIOUtils.readByte(buffer);
    compressor = ReadWriteIOUtils.readByte(buffer);
    mayAlterAudit = ReadWriteIOUtils.readBoolean(buffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeAlterEncodingCompressorPlan that = (PipeAlterEncodingCompressorPlan) o;
    return this.patternTreeBytes.equals(that.patternTreeBytes)
        && this.encoding == that.encoding
        && this.compressor == that.compressor
        && this.mayAlterAudit == that.mayAlterAudit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(patternTreeBytes, encoding, compressor, mayAlterAudit);
  }

  @Override
  public String toString() {
    return "PipeAlterEncodingCompressorPlan{"
        + "patternTreeBytes="
        + patternTreeBytes
        + ", encoding="
        + encoding
        + ", compressor="
        + compressor
        + '}';
  }
}
