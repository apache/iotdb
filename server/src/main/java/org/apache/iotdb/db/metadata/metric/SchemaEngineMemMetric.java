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
package org.apache.iotdb.db.metadata.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class SchemaEngineMemMetric implements ISchemaEngineMetric {

  private static final String TIME_SERES_CNT = "timeSeries";
  private static final String TOTAL_MEM_USAGE = "schema_region_total_mem_usage";
  private static final String MEM_CAPACITY = "schema_region_mem_capacity";
  private static final String REGION_NUMBER = "schema_region_number";

  private static final String SCHEMA_CONSENSUS = "schema_region_consensus";
  private static final String SCHEMA_ENGINE_MODE = "schema_engine_mode";

  private final MemSchemaEngineStatistics engineStatistics;

  public SchemaEngineMemMetric(MemSchemaEngineStatistics engineStatistics) {
    this.engineStatistics = engineStatistics;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.QUANTITY.toString(),
        MetricLevel.CORE,
        engineStatistics,
        ISchemaEngineStatistics::getTotalSeriesNumber,
        Tag.NAME.toString(),
        TIME_SERES_CNT);
    metricService.createAutoGauge(
        Metric.MEM.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        ISchemaEngineStatistics::getMemoryUsage,
        Tag.NAME.toString(),
        "schema_region_total_usage");
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        ISchemaEngineStatistics::getMemoryUsage,
        Tag.NAME.toString(),
        TOTAL_MEM_USAGE);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        ISchemaEngineStatistics::getMemoryCapacity,
        Tag.NAME.toString(),
        MEM_CAPACITY);
    metricService.createAutoGauge(
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        engineStatistics,
        ISchemaEngineStatistics::getSchemaRegionNumber,
        Tag.NAME.toString(),
        REGION_NUMBER);
    metricService.gauge(
        SchemaEngineMode.valueOf(IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode())
            .getCode(),
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        SCHEMA_ENGINE_MODE);
    metricService.gauge(
        getSchemaRegionConsensusProtocol(),
        Metric.SCHEMA_ENGINE.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        SCHEMA_CONSENSUS);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.QUANTITY.toString(), Tag.NAME.toString(), "timeSeries");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEM.toString(),
        Tag.NAME.toString(),
        "schema_region_total_usage");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SCHEMA_ENGINE.toString(),
        Tag.NAME.toString(),
        TOTAL_MEM_USAGE);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), MEM_CAPACITY);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SCHEMA_ENGINE.toString(), Tag.NAME.toString(), REGION_NUMBER);
  }

  /** Encode SchemaRegionConsensusProtocol to ordinal */
  private int getSchemaRegionConsensusProtocol() {
    switch (IoTDBDescriptor.getInstance().getConfig().getSchemaRegionConsensusProtocolClass()) {
      case ConsensusFactory.RATIS_CONSENSUS:
        return 0;
      case ConsensusFactory.SIMPLE_CONSENSUS:
        return 1;
      default:
        throw new IllegalArgumentException();
    }
  }
}
