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

package org.apache.iotdb.cluster.utils;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionUtils {

  private static final Logger logger = LoggerFactory.getLogger(PartitionUtils.class);
  private PartitionUtils() {
    // util class
  }

  /**
   * Localplan only be executed locally.
   * @param plan
   * @return
   */
  public static boolean isLocalPlan(PhysicalPlan plan) {
    return plan instanceof LoadDataPlan
        || plan instanceof OperateFilePlan
        || (plan instanceof ShowPlan
              && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.DYNAMIC_PARAMETER))
        || (plan instanceof ShowPlan
              && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.FLUSH_TASK_INFO))
        || (plan instanceof ShowPlan
              && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.VERSION));
  }

  /**
   * GlobalPlan will be executed on all nodes.
   * @param plan
   * @return
   */
  public static boolean isGlobalPlan(PhysicalPlan plan) {
    // TODO-Cluster#348: support more plans
    return plan instanceof SetStorageGroupPlan
          || plan instanceof SetTTLPlan
          || plan instanceof ShowTTLPlan
          || plan instanceof LoadConfigurationPlan
          || plan instanceof DeleteTimeSeriesPlan
          //delete timeseries plan is global because all nodes may have its data
          || plan instanceof AuthorPlan
    ;
  }

  public static int calculateStorageGroupSlot(String storageGroupName, long timestamp,
      int slotNum) {
    long partitionInstance = StorageEngine.fromTimeToTimePartition(timestamp);
    int hash = Murmur128Hash.hash(storageGroupName, partitionInstance, HASH_SALT);
    return Math.abs(hash % slotNum);
  }

  public static BatchInsertPlan copy(BatchInsertPlan plan, long[] times, Object[] values) {
    BatchInsertPlan newPlan = new BatchInsertPlan(plan.getDeviceId(), plan.getMeasurements());
    newPlan.setDataTypes(plan.getDataTypes());
    //according to TSServiceImpl.insertBatch(), only the deviceId, measreuments, dataTypes,
    //times, columns, and rowCount are need to be maintained.
    newPlan.setColumns(values);
    newPlan.setTimes(times);
    newPlan.setRowCount(times.length);
    return newPlan;
  }


}
