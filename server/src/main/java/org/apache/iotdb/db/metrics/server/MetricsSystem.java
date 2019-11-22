/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.iotdb.db.metrics.server;

import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metrics.sink.MetricsServletSink;
import org.apache.iotdb.db.metrics.sink.Sink;
import org.apache.iotdb.db.metrics.source.MetricsSource;
import org.apache.iotdb.db.metrics.source.Source;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.MonitorConstants.SystemMetrics;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsSystem implements IStatistic {

  private ArrayList<Sink> sinks;
  private ArrayList<Source> sources;
  private MetricRegistry metricRegistry;
  private ServerArgument serverArgument;
  private StorageEngine storageEngine;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String METRIC_PREFIX = MonitorConstants.SYSTEM_METRIC_PREFIX;
  private static final Logger logger = LoggerFactory.getLogger(MetricsSystem.class);

  public MetricsSystem(ServerArgument serverArgument) {
    this.sinks = new ArrayList<>();
    this.sources = new ArrayList<>();
    this.metricRegistry = new MetricRegistry();
    this.serverArgument = serverArgument;
    if (config.isEnableStatMonitor()) {
      storageEngine = StorageEngine.getInstance();
      StatMonitor statMonitor = StatMonitor.getInstance();
      registerStatMetadata();
      statMonitor.registerStatistics(METRIC_PREFIX, this);
    }
  }

  public ServerArgument getServerArgument() {
    return serverArgument;
  }

  public void setServerArgument(ServerArgument serverArgument) {
    this.serverArgument = serverArgument;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public ServletContextHandler getServletHandlers() {
    return new MetricsServletSink(metricRegistry).getHandler();
  }

  public void start() {
    registerSource();
    registerSinks();
    sinks.forEach(sink -> sink.start());
  }

  public void stop() {
    sinks.forEach(sink -> sink.stop());
  }

  public void report() {
    sinks.forEach(sink -> sink.report());
  }

  public void registerSource() {
    MetricsSource source = new MetricsSource(serverArgument, metricRegistry);
    source.registerInfo();
    sources.add(source);
  }

  public void registerSinks() {}

  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    TSRecord tsRecord = StatMonitor
        .convertToTSRecord(getStatParamsHashMap(), METRIC_PREFIX);
    HashMap<String, TSRecord> ret = new HashMap<>();
    ret.put(METRIC_PREFIX, tsRecord);
    return ret;
  }

  @Override
  public void registerStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (SystemMetrics kind : SystemMetrics.values()) {
      String seriesPath = METRIC_PREFIX
          + IoTDBConstant.PATH_SEPARATOR
          + kind.name();
      hashMap.put(seriesPath, MonitorConstants.DATA_TYPE_INT64);
      Path path = new Path(seriesPath);
      try {
        storageEngine.addTimeSeries(path, TSDataType.valueOf(MonitorConstants.DATA_TYPE_INT64),
            TSEncoding.valueOf("RLE"), CompressionType.valueOf(
                TSFileDescriptor.getInstance().getConfig().getCompressor()),
            Collections.emptyMap());
      } catch (StorageEngineException e) {
        logger.error("Register metrics of {} into storageEngine Failed.", this.getClass().getName(),
            e);
      }
    }
    StatMonitor.getInstance().registerMonitorTimeSeries(hashMap);
  }

  @Override
  public Map<String, Object> getStatParamsHashMap() {
    Map<String, Object> statParamsMap = new HashMap<>();
    statParamsMap.put(SystemMetrics.CPU_USAGE.name(), (long) serverArgument.getCpuRatio());
    statParamsMap.put(SystemMetrics.FREE_MEM.name(), serverArgument.freeMemory());
    statParamsMap.put(SystemMetrics.MAX_MEM.name(), serverArgument.maxMemory());
    statParamsMap.put(SystemMetrics.TOTAL_MEM.name(), serverArgument.totalMemory());
    statParamsMap.put(SystemMetrics.TOTAL_PHYSICAL_MEM.name(), serverArgument.totalPhysicalMemory());
    statParamsMap.put(SystemMetrics.FREE_PHYSICAL_MEM.name(), serverArgument.freePhysicalMemory());
    statParamsMap.put(SystemMetrics.USED_PHYSICAL_MEM.name(), serverArgument.usedPhysicalMemory());
    return statParamsMap;
  }
}
