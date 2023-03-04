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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class DeactivateTemplatePlan extends PhysicalPlan {

  String templateName;
  PartialPath prefixPath;
  transient List<PartialPath> pathToDeactivate;

  public DeactivateTemplatePlan() {
    super(OperatorType.DEACTIVATE_TEMPLATE);
  }

  /**
   * Inverse process of {@link ActivateTemplatePlan}, may delete time series data as side effect.
   *
   * <p><b>Before this plan being executed, it is compulsory to set {@link #pathToDeactivate} with
   * {@linkplain org.apache.iotdb.db.metadata.MManager#getPathsUsingTemplateUnderPrefix}, otherwise
   * it may deactivate nothing.</b>
   *
   * @param templateName template to delete.
   * @param prefixPath prefix of paths expected to set NOT using template, in pattern manner with
   *     wildcard.
   */
  public DeactivateTemplatePlan(String templateName, PartialPath prefixPath) {
    super(OperatorType.DEACTIVATE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = prefixPath;
    this.pathToDeactivate = null;
  }

  // shortcut for uts
  public DeactivateTemplatePlan(String templateName, String prefixPath)
      throws IllegalPathException {
    super(OperatorType.DEACTIVATE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = new PartialPath(prefixPath);
    this.pathToDeactivate = null;
  }

  @Override
  public List<PartialPath> getPaths() {
    return pathToDeactivate;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.pathToDeactivate = paths;
  }

  @Override
  public List<? extends PartialPath> getAuthPaths() {
    return Collections.singletonList(prefixPath);
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public String getTemplateName() {
    return templateName;
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.DEACTIVATE_TEMPLATE.ordinal());
    ReadWriteIOUtils.write(templateName, buffer);
    ReadWriteIOUtils.write(prefixPath.getFullPath(), buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    templateName = readString(buffer);
    prefixPath = new PartialPath(readString(buffer));
    index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.DEACTIVATE_TEMPLATE.ordinal());
    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(prefixPath.getFullPath(), stream);
    stream.writeLong(index);
  }
}
