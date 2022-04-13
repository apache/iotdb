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
package org.apache.iotdb.db.doublelive;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;

public class OperationSyncPlanTypeUtils {

  public static OperationSyncPlanType getOperationSyncPlanType(PhysicalPlan plan) {
    if (plan instanceof SetStorageGroupPlan
        || plan instanceof DeleteStorageGroupPlan
        || plan instanceof CreateTimeSeriesPlan
        || plan instanceof CreateMultiTimeSeriesPlan
        || plan instanceof CreateAlignedTimeSeriesPlan
        || plan instanceof DeleteTimeSeriesPlan
        || plan instanceof AlterTimeSeriesPlan) {
      return OperationSyncPlanType.EPlan;
    } else if (plan instanceof DeletePlan) {
      return OperationSyncPlanType.IPlan;
    } else if (plan instanceof InsertPlan) {
      return OperationSyncPlanType.NPlan;
    }
    return null;
  }

  public enum OperationSyncPlanType {
    EPlan,
    IPlan,
    NPlan
  }
}
