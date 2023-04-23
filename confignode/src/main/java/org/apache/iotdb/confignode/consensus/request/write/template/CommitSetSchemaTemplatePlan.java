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

package org.apache.iotdb.confignode.consensus.request.write.template;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CommitSetSchemaTemplatePlan extends ConfigPhysicalPlan {

  private String name;
  private String path;

  private boolean isRollback = false;

  public CommitSetSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.CommitSetSchemaTemplate);
  }

  public CommitSetSchemaTemplatePlan(String name, String path) {
    super(ConfigPhysicalPlanType.CommitSetSchemaTemplate);
    this.name = name;
    this.path = path;
  }

  public CommitSetSchemaTemplatePlan(String name, String path, boolean isRollback) {
    super(ConfigPhysicalPlanType.CommitSetSchemaTemplate);
    this.name = name;
    this.path = path;
    this.isRollback = isRollback;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(name, stream);
    ReadWriteIOUtils.write(path, stream);
    ReadWriteIOUtils.write(isRollback, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.name = ReadWriteIOUtils.readString(buffer);
    this.path = ReadWriteIOUtils.readString(buffer);
    this.isRollback = ReadWriteIOUtils.readBool(buffer);
  }
}
