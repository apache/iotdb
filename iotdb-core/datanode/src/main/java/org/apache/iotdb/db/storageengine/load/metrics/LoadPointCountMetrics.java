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

package org.apache.iotdb.db.storageengine.load.metrics;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.metrics.utils.MetricLevel;

/** Counts the points a LOAD imported, for the region and for its leader. */
public final class LoadPointCountMetrics {

  private LoadPointCountMetrics() {}

  public static void updateWritePointCountMetrics(
      final DataRegion dataRegion,
      final String databaseName,
      final long writePointCount,
      final boolean isGeneratedByIoTConsensusV2Leader) {
    MemTableFlushTask.recordFlushPointsMetricInternal(
        writePointCount, databaseName, dataRegion.getDataRegionIdString());
    MetricService.getInstance()
        .count(
            writePointCount,
            Metric.QUANTITY.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            Metric.POINTS_IN.toString(),
            Tag.DATABASE.toString(),
            databaseName,
            Tag.REGION.toString(),
            dataRegion.getDataRegionIdString(),
            Tag.TYPE.toString(),
            Metric.LOAD_POINT_COUNT.toString());
    // Because we cannot accurately judge who is the leader here,
    // we directly divide the writePointCount by the replicationNum to ensure the
    // correctness of this metric, which will be accurate in most cases
    final int replicationNum =
        DataRegionConsensusImpl.getInstance()
            .getReplicationNum(
                ConsensusGroupId.Factory.create(
                    TConsensusGroupType.DataRegion.getValue(),
                    Integer.parseInt(dataRegion.getDataRegionIdString())));
    // It may happen that the replicationNum is 0 when load and db deletion occurs
    // concurrently, so we can just not to count the number of points in this case
    if (replicationNum != 0 && !isGeneratedByIoTConsensusV2Leader) {
      MetricService.getInstance()
          .count(
              writePointCount / replicationNum,
              Metric.LEADER_QUANTITY.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              Metric.POINTS_IN.toString(),
              Tag.DATABASE.toString(),
              databaseName,
              Tag.REGION.toString(),
              dataRegion.getDataRegionIdString(),
              Tag.TYPE.toString(),
              Metric.LOAD_POINT_COUNT.toString());
    }
  }
}
