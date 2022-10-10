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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ActivateTemplateInClusterPlan extends PhysicalPlan
    implements IActivateTemplateInClusterPlan {

  private PartialPath activatePath;
  private int templateSetLevel;
  private int templateId;
  private boolean isAligned;

  public ActivateTemplateInClusterPlan() {
    super(Operator.OperatorType.ACTIVATE_TEMPLATE_IN_CLUSTER);
  }

  public ActivateTemplateInClusterPlan(
      PartialPath activatePath, int templateSetLevel, int templateId) {
    this.activatePath = activatePath;
    this.templateSetLevel = templateSetLevel;
    this.templateId = templateId;
  }

  public PartialPath getActivatePath() {
    return activatePath;
  }

  @Override
  public void setActivatePath(PartialPath activatePath) {
    this.activatePath = activatePath;
  }

  public PartialPath getPathSetTemplate() {
    return new PartialPath(Arrays.copyOf(activatePath.getNodes(), templateSetLevel + 1));
  }

  public int getTemplateId() {
    return templateId;
  }

  @Override
  public void setTemplateId(int templateSetLevel) {
    this.templateSetLevel = templateSetLevel;
  }

  public int getTemplateSetLevel() {
    return templateSetLevel;
  }

  @Override
  public void setTemplateSetLevel(int templateId) {
    this.templateId = templateId;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.singletonList(activatePath);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.ACTIVATE_TEMPLATE_IN_CLUSTER.ordinal());
    ReadWriteIOUtils.write(activatePath.getFullPath(), stream);
    ReadWriteIOUtils.write(templateSetLevel, stream);
    ReadWriteIOUtils.write(templateId, stream);
    ReadWriteIOUtils.write(isAligned, stream);
    stream.writeLong(index);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.ACTIVATE_TEMPLATE_IN_CLUSTER.ordinal());
    ReadWriteIOUtils.write(activatePath.getFullPath(), buffer);
    ReadWriteIOUtils.write(templateSetLevel, buffer);
    ReadWriteIOUtils.write(templateId, buffer);
    ReadWriteIOUtils.write(isAligned, buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
    activatePath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    templateSetLevel = ReadWriteIOUtils.readInt(buffer);
    templateId = ReadWriteIOUtils.readInt(buffer);
    isAligned = ReadWriteIOUtils.readBool(buffer);
    index = buffer.getLong();
  }
}
