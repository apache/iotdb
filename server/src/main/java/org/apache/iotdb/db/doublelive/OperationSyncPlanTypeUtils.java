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
import org.apache.iotdb.db.qp.physical.sys.*;

public class OperationSyncPlanTypeUtils {

  public static OperationSyncPlanType getOperationSyncPlanType(PhysicalPlan plan) {
    if (plan instanceof SetStorageGroupPlan
        || plan instanceof DeleteStorageGroupPlan
        || plan instanceof CreateTimeSeriesPlan
        || plan instanceof CreateMultiTimeSeriesPlan
        || plan instanceof CreateAlignedTimeSeriesPlan
        || plan instanceof DeleteTimeSeriesPlan
        || plan instanceof AlterTimeSeriesPlan
        || plan instanceof ActivateTemplatePlan
        || plan instanceof AppendTemplatePlan
        || plan instanceof CreateTemplatePlan
        || plan instanceof DeactivateTemplatePlan
        || plan instanceof DropTemplatePlan
        || plan instanceof PruneTemplatePlan
        || plan instanceof SetTemplatePlan
        || plan instanceof UnsetTemplatePlan
        || plan instanceof SetTTLPlan
        || plan instanceof CreateContinuousQueryPlan
        || plan instanceof DataAuthPlan
        || plan instanceof DropContinuousQueryPlan
        || plan instanceof ChangeAliasPlan
        || plan instanceof ChangeTagOffsetPlan) {

      return OperationSyncPlanType.DDLPlan;
    } else if (plan instanceof DeletePlan || plan instanceof InsertPlan) {
      return OperationSyncPlanType.DMLPlan;
    }
    return null;
  }

  public enum OperationSyncPlanType {
    DDLPlan, // Create, update and delete schema
    DMLPlan // insert and delete data
  }
}
