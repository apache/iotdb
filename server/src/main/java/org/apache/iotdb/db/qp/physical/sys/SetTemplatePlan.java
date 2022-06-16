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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SetTemplatePlan extends PhysicalPlan {
  String templateName;
  String prefixPath;

  public SetTemplatePlan() {
    super(OperatorType.SET_TEMPLATE);
  }

  public SetTemplatePlan(String templateName, String prefixPath) throws IllegalPathException {
    super(OperatorType.SET_TEMPLATE);

    String[] pathNodes = PathUtils.splitPathToDetachedNodes(prefixPath);
    for (String s : pathNodes) {
      if (s.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
          || s.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        throw new IllegalPathException(
            prefixPath, "template cannot be set on a path with wildcard.");
      }
    }

    this.templateName = templateName;
    this.prefixPath = prefixPath;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  public String getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(String prefixPath) {
    this.prefixPath = prefixPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SET_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(templateName, buffer);
    ReadWriteIOUtils.write(prefixPath, buffer);

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    templateName = ReadWriteIOUtils.readString(buffer);
    prefixPath = ReadWriteIOUtils.readString(buffer);

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.SET_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(prefixPath, stream);

    stream.writeLong(index);
  }
}
