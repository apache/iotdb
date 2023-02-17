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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;

public class SettlePlan extends PhysicalPlan {
  PartialPath sgPath;
  String tsFilePath;
  boolean isSgPath;

  public SettlePlan(PartialPath sgPath) {
    super(OperatorType.SETTLE);
    this.sgPath = sgPath;
    setIsSgPath(true);
  }

  public SettlePlan(String tsFilePath) {
    super(OperatorType.SETTLE);
    this.tsFilePath = tsFilePath;
    setIsSgPath(false);
  }

  public boolean isSgPath() {
    return isSgPath;
  }

  public void setIsSgPath(boolean isSgPath) {
    this.isSgPath = isSgPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(sgPath);
  }

  public PartialPath getSgPath() {
    return sgPath;
  }

  public String getTsFilePath() {
    return tsFilePath;
  }
}
