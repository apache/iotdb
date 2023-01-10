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
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeactivateTemplatePlan;

import java.util.List;
import java.util.Map;

public class DeactivateTemplatePlanImpl implements IDeactivateTemplatePlan {

  private Map<PartialPath, List<Integer>> templateSetInfo;

  DeactivateTemplatePlanImpl() {}

  DeactivateTemplatePlanImpl(Map<PartialPath, List<Integer>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
  }

  @Override
  public Map<PartialPath, List<Integer>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  @Override
  public void setTemplateSetInfo(Map<PartialPath, List<Integer>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
  }
}
