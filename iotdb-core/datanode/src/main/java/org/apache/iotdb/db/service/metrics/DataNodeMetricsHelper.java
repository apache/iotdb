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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.concurrent.ThreadModule;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.ThreadPoolMetrics;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.service.metric.cpu.CpuUsageMetrics;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeMetrics;
import org.apache.iotdb.db.protocol.thrift.handler.RPCServiceThriftHandlerMetrics;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet;
import org.apache.iotdb.db.queryengine.metric.DriverSchedulerMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.service.metrics.memory.GlobalMemoryMetrics;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesSizeMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileMemMetricSet;
import org.apache.iotdb.db.subscription.metric.SubscriptionMetrics;
import org.apache.iotdb.metrics.metricsets.UpTimeMetrics;
import org.apache.iotdb.metrics.metricsets.disk.DiskMetrics;
import org.apache.iotdb.metrics.metricsets.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.logback.LogbackMetrics;
import org.apache.iotdb.metrics.metricsets.net.NetMetrics;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataNodeMetricsHelper {
  /** Bind predefined metric sets into DataNode. */
  public static void bind() {
    MetricService metricService = MetricService.getInstance();
    metricService.addMetricSet(new UpTimeMetrics());
    metricService.addMetricSet(new JvmMetrics());
    metricService.addMetricSet(ThreadPoolMetrics.getInstance());
    metricService.addMetricSet(new LogbackMetrics());
    metricService.addMetricSet(FileMetrics.getInstance());
    metricService.addMetricSet(CompactionMetrics.getInstance());
    metricService.addMetricSet(new ProcessMetrics());
    metricService.addMetricSet(new DiskMetrics(IoTDBConstant.DN_ROLE));
    metricService.addMetricSet(new NetMetrics(IoTDBConstant.DN_ROLE));
    metricService.addMetricSet(ClientManagerMetrics.getInstance());
    metricService.addMetricSet(RPCServiceThriftHandlerMetrics.getInstance());
    initCpuMetrics(metricService);
    initSystemMetrics(metricService);
    metricService.addMetricSet(WritingMetrics.getInstance());

    // bind query related metrics
    metricService.addMetricSet(QueryPlanCostMetricSet.getInstance());
    metricService.addMetricSet(SeriesScanCostMetricSet.getInstance());
    metricService.addMetricSet(QueryExecutionMetricSet.getInstance());
    metricService.addMetricSet(QueryResourceMetricSet.getInstance());
    metricService.addMetricSet(DataExchangeCostMetricSet.getInstance());
    metricService.addMetricSet(DataExchangeCountMetricSet.getInstance());
    metricService.addMetricSet(DriverSchedulerMetricSet.getInstance());
    metricService.addMetricSet(QueryRelatedResourceMetricSet.getInstance());

    // bind performance overview related metrics
    metricService.addMetricSet(PerformanceOverviewMetrics.getInstance());

    // bind gc metrics
    metricService.addMetricSet(JvmGcMonitorMetrics.getInstance());

    // bind pipe related metrics
    metricService.addMetricSet(PipeDataNodeMetrics.getInstance());

    // bind load tsfile memory related metrics
    metricService.addMetricSet(LoadTsFileMemMetricSet.getInstance());

    // bind subscription related metrics
    metricService.addMetricSet(SubscriptionMetrics.getInstance());

    // bind load related metrics
    metricService.addMetricSet(LoadTsFileCostMetricsSet.getInstance());
    metricService.addMetricSet(ActiveLoadingFilesNumberMetricsSet.getInstance());
    metricService.addMetricSet(ActiveLoadingFilesSizeMetricsSet.getInstance());

    // bind memory related metrics
    metricService.addMetricSet(GlobalMemoryMetrics.getInstance());
  }

  private static void initSystemMetrics(MetricService metricService) {
    ArrayList<String> diskDirs = new ArrayList<>();
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getConsensusDir());
    diskDirs.addAll(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    diskDirs.addAll(Arrays.asList(CommonDescriptor.getInstance().getConfig().getWalDirs()));
    diskDirs.add(CommonDescriptor.getInstance().getConfig().getSyncDir());
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getSortTmpDir());
    SystemMetrics.getInstance().setDiskDirs(diskDirs);
    metricService.addMetricSet(SystemMetrics.getInstance());
  }

  private static void initCpuMetrics(MetricService metricService) {
    List<String> threadModules = new ArrayList<>();
    Arrays.stream(ThreadModule.values()).forEach(x -> threadModules.add(x.toString()));
    List<String> pools = new ArrayList<>();
    Arrays.stream(ThreadName.values()).forEach(x -> pools.add(x.name()));
    metricService.addMetricSet(
        new CpuUsageMetrics(
            threadModules,
            pools,
            x -> ThreadName.getModuleTheThreadBelongs(x).toString(),
            x -> ThreadName.getThreadPoolTheThreadBelongs(x).name()));
  }
}
