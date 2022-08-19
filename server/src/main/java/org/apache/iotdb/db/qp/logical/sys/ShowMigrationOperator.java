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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowMigrationPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.List;

public class ShowMigrationOperator extends ShowOperator {
  private List<PartialPath> storageGroups;

  public ShowMigrationOperator(List<PartialPath> storageGroups) {
    super(SQLConstant.TOK_SHOW, OperatorType.SHOW_MIGRATION);
    this.storageGroups = storageGroups;
  }

  public List<PartialPath> getStorageGroups() {
    return storageGroups;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator) {
    return new ShowMigrationPlan(storageGroups);
  }
}
