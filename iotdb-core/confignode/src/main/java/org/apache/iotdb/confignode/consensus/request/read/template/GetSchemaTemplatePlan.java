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

package org.apache.iotdb.confignode.consensus.request.read.template;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetSchemaTemplatePlan extends ConfigPhysicalPlan {

  private String templateName;

  public GetSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.GetSchemaTemplate);
  }

  public GetSchemaTemplatePlan(String templateName) {
    this();
    this.templateName = templateName;
  }

  public String getTemplateName() {
    return templateName;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(templateName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.templateName = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetSchemaTemplatePlan that = (GetSchemaTemplatePlan) o;
    return this.templateName.equalsIgnoreCase(this.templateName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateName);
  }
}
