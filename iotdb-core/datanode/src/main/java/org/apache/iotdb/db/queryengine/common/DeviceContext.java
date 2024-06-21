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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DeviceContext {
  private boolean isAligned;
  private final int templateId;

  public DeviceContext(DeviceSchemaInfo deviceSchemaInfo) {
    this.isAligned = deviceSchemaInfo.isAligned();
    this.templateId = deviceSchemaInfo.getTemplateId();
  }

  public DeviceContext(boolean isAligned, int templateId) {
    this.isAligned = isAligned;
    this.templateId = templateId;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public int getTemplateId() {
    return templateId;
  }

  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(isAligned, byteBuffer);
    ReadWriteIOUtils.write(templateId, byteBuffer);
  }

  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isAligned, stream);
    ReadWriteIOUtils.write(templateId, stream);
  }

  public static DeviceContext deserialize(ByteBuffer buffer) {
    boolean isAligned = ReadWriteIOUtils.readBool(buffer);
    int templateId = ReadWriteIOUtils.readInt(buffer);
    return new DeviceContext(isAligned, templateId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeviceContext that = (DeviceContext) o;
    return isAligned == that.isAligned && templateId == that.templateId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(isAligned, templateId);
  }
}
