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

package org.apache.iotdb.db.mpp.plan.statement.internal;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// This is only used for auto activate template on multi devices while inserting data
public class InternalBatchActivateTemplateStatement extends Statement {

  // devicePath -> <Template, TemplateSetPath>
  private Map<PartialPath, Pair<Template, PartialPath>> deviceMap;

  public InternalBatchActivateTemplateStatement(
      Map<PartialPath, Pair<Template, PartialPath>> deviceMap) {
    super();
    setType(StatementType.INTERNAL_BATCH_ACTIVATE_TEMPLATE);
    this.deviceMap = deviceMap;
  }

  public Map<PartialPath, Pair<Template, PartialPath>> getDeviceMap() {
    return deviceMap;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return new ArrayList<>(deviceMap.keySet());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInternalBatchActivateTemplate(this, context);
  }
}
