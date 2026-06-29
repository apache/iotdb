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

package org.apache.iotdb.confignode.consensus.request.write.pipe.payload;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeDeactivateTemplatePlan extends ConfigPhysicalPlan {

  private Map<PartialPath, List<Template>> templateSetInfo;

  public PipeDeactivateTemplatePlan() {
    super(ConfigPhysicalPlanType.PipeDeactivateTemplate);
  }

  public PipeDeactivateTemplatePlan(final Map<PartialPath, List<Template>> templateSetInfo) {
    super(ConfigPhysicalPlanType.PipeDeactivateTemplate);
    this.templateSetInfo = templateSetInfo;
  }

  public Map<PartialPath, List<Template>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(templateSetInfo.size(), stream);
    for (final Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (final Template template : entry.getValue()) {
        template.serialize(stream);
      }
    }
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    final int size = ReadWriteIOUtils.readInt(buffer);
    templateSetInfo = new HashMap<>();
    for (int i = 0; i < size; i++) {
      final PartialPath pattern = (PartialPath) PathDeserializeUtil.deserialize(buffer);
      final int templateNum = ReadWriteIOUtils.readInt(buffer);
      final List<Template> templateList = new ArrayList<>(templateNum);
      for (int j = 0; j < templateNum; j++) {
        Template template = new Template();
        template.deserialize(buffer);
        templateList.add(template);
      }
      templateSetInfo.put(pattern, templateList);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeDeactivateTemplatePlan that = (PipeDeactivateTemplatePlan) obj;
    return templateSetInfo.equals(that.templateSetInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateSetInfo);
  }

  @Override
  public String toString() {
    return "PipeDeactivateTemplatePlan{" + "templateSetInfo='" + templateSetInfo + "'}";
  }
}
