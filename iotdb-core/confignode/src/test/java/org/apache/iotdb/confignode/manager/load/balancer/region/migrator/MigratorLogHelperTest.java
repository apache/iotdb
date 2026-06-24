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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MigratorLogHelperTest {

  private static TRegionReplicaSet regionOf(TConsensusGroupType type, int id) {
    return new TRegionReplicaSet().setRegionId(new TConsensusGroupId(type, id));
  }

  /**
   * The fix for V2-1016 distinguishes the two LOAD BALANCE passes (DataRegion vs SchemaRegion) in
   * the log. The migrator builds its log tag from this method, so it must return the consensus
   * group type name of the regions being balanced.
   */
  @Test
  public void regionTypeTagReturnsDataRegion() {
    List<TRegionReplicaSet> dataRegions =
        Collections.singletonList(regionOf(TConsensusGroupType.DataRegion, 1));
    assertEquals("DataRegion", MigratorLogHelper.regionTypeTag(dataRegions));
  }

  @Test
  public void regionTypeTagReturnsSchemaRegion() {
    List<TRegionReplicaSet> schemaRegions =
        Collections.singletonList(regionOf(TConsensusGroupType.SchemaRegion, 1));
    assertEquals("SchemaRegion", MigratorLogHelper.regionTypeTag(schemaRegions));
  }

  @Test
  public void regionTypeTagFallsBackWhenEmpty() {
    assertEquals("UnknownRegion", MigratorLogHelper.regionTypeTag(Collections.emptyList()));
    assertEquals("UnknownRegion", MigratorLogHelper.regionTypeTag(null));
  }
}
