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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetTemplateSetInfoPlan extends ConfigPhysicalPlan {

  private List<PartialPath> patternList;

  public GetTemplateSetInfoPlan() {
    super(ConfigPhysicalPlanType.GetTemplateSetInfo);
  }

  public GetTemplateSetInfoPlan(List<PartialPath> patternList) {
    super(ConfigPhysicalPlanType.GetTemplateSetInfo);
    this.patternList = patternList;
  }

  public List<PartialPath> getPatternList() {
    return patternList;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(patternList.size(), stream);
    for (PartialPath pattern : patternList) {
      pattern.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    patternList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      patternList.add((PartialPath) PathDeserializeUtil.deserialize(buffer));
    }
  }
}
