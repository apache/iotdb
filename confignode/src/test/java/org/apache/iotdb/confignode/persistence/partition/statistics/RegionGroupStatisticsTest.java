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
package org.apache.iotdb.confignode.persistence.partition.statistics;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionStatistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RegionGroupStatisticsTest {

  @Test
  public void RegionGroupStatisticsSerDeTest() throws IOException {
    Map<Integer, RegionStatistics> regionStatisticsMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      regionStatisticsMap.put(i, new RegionStatistics(RegionStatus.Unknown));
    }

    RegionGroupStatistics statistics0 =
        new RegionGroupStatistics(RegionGroupStatus.Disabled, regionStatisticsMap);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      statistics0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      RegionGroupStatistics statistics1 = new RegionGroupStatistics();
      statistics1.deserialize(buffer);
      Assert.assertEquals(statistics0, statistics1);
    }
  }
}
