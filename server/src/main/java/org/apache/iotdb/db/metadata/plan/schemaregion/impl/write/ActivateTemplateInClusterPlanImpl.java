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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;

public class ActivateTemplateInClusterPlanImpl implements IActivateTemplateInClusterPlan {

  private PartialPath activatePath;
  private int templateSetLevel;
  private int templateId;
  private boolean isAligned;

  ActivateTemplateInClusterPlanImpl() {}

  ActivateTemplateInClusterPlanImpl(
      PartialPath activatePath, int templateSetLevel, int templateId) {
    this.activatePath = activatePath;
    this.templateSetLevel = templateSetLevel;
    this.templateId = templateId;
  }

  public PartialPath getActivatePath() {
    return activatePath;
  }

  @Override
  public void setActivatePath(PartialPath activatePath) {
    this.activatePath = activatePath;
  }

  @Override
  public int getTemplateId() {
    return templateId;
  }

  @Override
  public void setTemplateId(int templateId) {
    this.templateId = templateId;
  }

  @Override
  public int getTemplateSetLevel() {
    return templateSetLevel;
  }

  @Override
  public void setTemplateSetLevel(int templateSetLevel) {
    this.templateSetLevel = templateSetLevel;
  }

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }
}
