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

package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PartitionManagerRegionMaintainTest {

  @Test
  public void testCreateRegionCompletedStatus() {
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.REGION_ALREADY_EXISTS.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())));
  }

  @Test
  public void testDeleteRegionCompletedStatus() {
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.REGION_ALREADY_EXISTS.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode())));
  }
}
