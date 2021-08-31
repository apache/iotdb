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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SetSchemaTemplatePlan extends PhysicalPlan {
  String templateName;
  String prefixPath;

  public SetSchemaTemplatePlan() {
    super(false, OperatorType.SET_SCHEMA_TEMPLATE);
  }

  public SetSchemaTemplatePlan(String templateName, String prefixPath) {
    super(false, OperatorType.SET_SCHEMA_TEMPLATE);
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
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SET_SCHEMA_TEMPLATE.ordinal());

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
    stream.writeByte((byte) PhysicalPlanType.SET_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(prefixPath, stream);

    stream.writeLong(index);
  }
}
