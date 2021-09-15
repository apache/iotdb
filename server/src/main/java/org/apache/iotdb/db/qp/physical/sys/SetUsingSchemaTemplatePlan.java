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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SetUsingSchemaTemplatePlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(SetUsingSchemaTemplatePlan.class);
  PartialPath prefixPath;

  public SetUsingSchemaTemplatePlan() {
    super(false, OperatorType.SET_USING_SCHEMA_TEMPLATE);
  }

  public SetUsingSchemaTemplatePlan(PartialPath prefixPath) {
    super(false, OperatorType.SET_USING_SCHEMA_TEMPLATE);
    this.prefixPath = prefixPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SET_USING_SCHEMA_TEMPLATE.ordinal());
    ReadWriteIOUtils.write(prefixPath.getFullPath(), buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    String pathString = readString(buffer);
    try {
      prefixPath = new PartialPath(pathString);
    } catch (IllegalPathException e) {
      logger.error("Failed to deserialize device {} from buffer", pathString);
    }
    index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.SET_USING_SCHEMA_TEMPLATE.ordinal());
    ReadWriteIOUtils.write(prefixPath.getFullPath(), stream);
    stream.writeLong(index);
  }
}
