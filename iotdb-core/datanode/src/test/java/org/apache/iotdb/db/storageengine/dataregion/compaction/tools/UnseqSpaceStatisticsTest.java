/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.tools;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.Interval;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.UnseqSpaceStatistics;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

public class UnseqSpaceStatisticsTest {

  @Test
  public void test01() {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(1, 10));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(5, 15));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s2", new Interval(1, 10));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d2"), "s2", new Interval(1, 10));

    Assert.assertEquals(2, unseqSpaceStatistics.getChunkStatisticMap().size());
    Assert.assertEquals(
        2,
        unseqSpaceStatistics
            .getChunkStatisticMap()
            .get(IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"))
            .size());
    Assert.assertEquals(
        1,
        unseqSpaceStatistics
            .getChunkStatisticMap()
            .get(IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d2"))
            .size());
  }

  @Test
  public void test02() {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(1, 10));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(5, 15));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s2", new Interval(1, 10));
    unseqSpaceStatistics.updateMeasurement(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d2"), "s2", new Interval(1, 10));

    Assert.assertTrue(
        unseqSpaceStatistics.chunkHasOverlap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(1, 10)));
    Assert.assertFalse(
        unseqSpaceStatistics.chunkHasOverlap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s4", new Interval(1, 10)));
    Assert.assertFalse(
        unseqSpaceStatistics.chunkHasOverlap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d2"), "s1", new Interval(1, 10)));

    Assert.assertFalse(
        unseqSpaceStatistics.chunkHasOverlap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d3"), "s1", new Interval(1, 10)));
    Assert.assertFalse(
        unseqSpaceStatistics.chunkHasOverlap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), "s1", new Interval(21, 30)));
  }
}
