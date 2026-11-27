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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipeDescriptorTest {

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private int originalRequestSliceThresholdBytes;
  private int originalAirGapUdpSocketBufferSizeInBytes;

  @Before
  public void setUp() {
    originalRequestSliceThresholdBytes = config.getPipeSinkRequestSliceThresholdBytes();
    originalAirGapUdpSocketBufferSizeInBytes =
        config.getPipeAirGapReceiverUdpSocketBufferSizeInBytes();
  }

  @After
  public void tearDown() {
    config.setPipeSinkRequestSliceThresholdBytes(originalRequestSliceThresholdBytes);
    config.setPipeAirGapReceiverUdpSocketBufferSizeInBytes(
        originalAirGapUdpSocketBufferSizeInBytes);
  }

  @Test
  public void testPipeRequestSliceThresholdSupportsSinkAndConnectorKeys() {
    final TrimProperties connectorProperties = new TrimProperties();
    connectorProperties.setProperty("pipe_connector_request_slice_threshold_bytes", "123");
    PipeDescriptor.loadPipeInternalConfig(config, connectorProperties);
    Assert.assertEquals(123, config.getPipeSinkRequestSliceThresholdBytes());

    final TrimProperties sinkProperties = new TrimProperties();
    sinkProperties.setProperty("pipe_sink_request_slice_threshold_bytes", "456");
    PipeDescriptor.loadPipeInternalConfig(config, sinkProperties);
    Assert.assertEquals(456, config.getPipeSinkRequestSliceThresholdBytes());

    final TrimProperties bothProperties = new TrimProperties();
    bothProperties.setProperty("pipe_connector_request_slice_threshold_bytes", "123");
    bothProperties.setProperty("pipe_sink_request_slice_threshold_bytes", "456");
    PipeDescriptor.loadPipeInternalConfig(config, bothProperties);
    Assert.assertEquals(456, config.getPipeSinkRequestSliceThresholdBytes());
  }

  @Test
  public void testAirGapReceiverUdpSocketBufferIsLoadedFromGlobalProperties() {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("pipe_air_gap_receiver_udp_socket_buffer_size_in_bytes", "1048576");

    PipeDescriptor.loadPipeStaticConfig(config, properties);
    PipeDescriptor.loadPipeInternalConfig(config, properties);

    Assert.assertEquals(1048576, config.getPipeAirGapReceiverUdpSocketBufferSizeInBytes());
  }

  @Test
  public void testHardlinkDirectoryNamesCanBeRelative() {
    final String originalBaseDirName = config.getPipeHardlinkBaseDirName();
    final String originalTsFileDirName = config.getPipeHardlinkTsFileDirName();
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("pipe_hardlink_base_dir_name", "custom-pipe");
    properties.setProperty("pipe_hardlink_tsfile_dir_name", "custom-tsfile");

    try {
      PipeDescriptor.loadPipeStaticConfig(config, properties);
      Assert.assertEquals("custom-pipe", config.getPipeHardlinkBaseDirName());
      Assert.assertEquals("custom-tsfile", config.getPipeHardlinkTsFileDirName());
    } finally {
      config.setPipeHardlinkBaseDirName(originalBaseDirName);
      config.setPipeHardlinkTsFileDirName(originalTsFileDirName);
    }
  }

  @Test
  public void testInvalidTwoStageAggregateValuesFallBackToCurrentConfig() {
    final long originalMaxCombinerLiveTimeInMs =
        config.getTwoStageAggregateMaxCombinerLiveTimeInMs();
    final long originalDataRegionInfoCacheTimeInMs =
        config.getTwoStageAggregateDataRegionInfoCacheTimeInMs();
    final long originalSenderEndPointsCacheInMs =
        config.getTwoStageAggregateSenderEndPointsCacheInMs();
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("two_stage_aggregate_max_combiner_live_time_in_ms", "invalid");
    properties.setProperty("two_stage_aggregate_data_region_info_cache_time_in_ms", "-1");
    properties.setProperty("two_stage_aggregate_sender_end_points_cache_in_ms", "invalid");

    PipeDescriptor.loadPipeStaticConfig(config, properties);

    Assert.assertEquals(
        originalMaxCombinerLiveTimeInMs, config.getTwoStageAggregateMaxCombinerLiveTimeInMs());
    Assert.assertEquals(
        originalDataRegionInfoCacheTimeInMs,
        config.getTwoStageAggregateDataRegionInfoCacheTimeInMs());
    Assert.assertEquals(
        originalSenderEndPointsCacheInMs, config.getTwoStageAggregateSenderEndPointsCacheInMs());
  }
}
