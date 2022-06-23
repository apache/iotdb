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
import java.util.Arrays;
import java.util.List;

public class DeactivateTemplatePlan extends PhysicalPlan {

  String templateName;
  PartialPath prefixPath;
  PartialPath[] pathToDeactivate;

  public DeactivateTemplatePlan() {
    super(OperatorType.DEACTIVATE_TEMPLATE);
  }

  /**
   * Inverse process of {@link ActivateTemplatePlan}, may delete time series data as side effect.
   *
   * <p>It might be some redundant as paths can be derived from prefixPath. But to avoid traversal
   * when checking authority in {@linkplain org.apache.iotdb.db.service.thrift.impl.TSServiceImpl}
   * and traversal again when setting not using template in {@linkplain
   * org.apache.iotdb.db.metadata.MManager}, this implementation prefers to endure a little
   * redundancy.
   *
   * @param templateName template to delete.
   * @param prefixPath prefix of paths expected to set NOT using template, in pattern manner with
   *     wildcard.
   * @param paths using template under the prefix when this plan created.
   */
  public DeactivateTemplatePlan(
      String templateName, PartialPath prefixPath, List<PartialPath> paths) {
    super(OperatorType.DEACTIVATE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = prefixPath;
    this.pathToDeactivate = paths.toArray(new PartialPath[0]);
  }

  // shortcut for uts
  public DeactivateTemplatePlan(String templateName, String prefixPath, List<String> paths)
      throws IllegalPathException {
    super(OperatorType.DEACTIVATE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = new PartialPath(prefixPath);
    this.pathToDeactivate = new PartialPath[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      this.pathToDeactivate[i] = new PartialPath(paths.get(i));
    }
  }

  @Override
  public List<PartialPath> getPaths() {
    return Arrays.asList(pathToDeactivate);
  }

  @Override
  public List<? extends PartialPath> getAuthPaths() {
    return Arrays.asList(pathToDeactivate);
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
    ReadWriteIOUtils.write(pathToDeactivate.length, buffer);
    for (PartialPath path : pathToDeactivate) {
      ReadWriteIOUtils.write(path.getFullPath(), buffer);
    }
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    templateName = readString(buffer);
    prefixPath = new PartialPath(readString(buffer));
    pathToDeactivate = new PartialPath[ReadWriteIOUtils.readInt(buffer)];
    for (int i = 0; i < pathToDeactivate.length; i++) {
      pathToDeactivate[i] = new PartialPath(readString(buffer));
    }
    index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.DEACTIVATE_TEMPLATE.ordinal());
    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(prefixPath.getFullPath(), stream);
    ReadWriteIOUtils.write(pathToDeactivate.length, stream);
    for (PartialPath path : pathToDeactivate) {
      ReadWriteIOUtils.write(path.getFullPath(), stream);
    }
    stream.writeLong(index);
  }
}
