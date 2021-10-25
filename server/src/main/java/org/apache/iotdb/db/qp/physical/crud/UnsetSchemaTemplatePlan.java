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
 *
 */

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class UnsetSchemaTemplatePlan extends PhysicalPlan {

  String prefixPath;
  String templateName;

  public UnsetSchemaTemplatePlan() {
    super(false, Operator.OperatorType.UNSET_SCHEMA_TEMPLATE);
  }

  public UnsetSchemaTemplatePlan(String prefixPath, String templateName) {
    super(false, Operator.OperatorType.UNSET_SCHEMA_TEMPLATE);
    this.prefixPath = prefixPath;
    this.templateName = templateName;
  }

  public String getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(String prefixPath) {
    this.prefixPath = prefixPath;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.UNSET_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(prefixPath, buffer);
    ReadWriteIOUtils.write(templateName, buffer);

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    prefixPath = ReadWriteIOUtils.readString(buffer);
    templateName = ReadWriteIOUtils.readString(buffer);

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.UNSET_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(prefixPath, stream);
    ReadWriteIOUtils.write(templateName, stream);

    stream.writeLong(index);
  }
}
