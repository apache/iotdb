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

  @Before
  public void setUp() {
    originalRequestSliceThresholdBytes = config.getPipeSinkRequestSliceThresholdBytes();
  }

  @After
  public void tearDown() {
    config.setPipeSinkRequestSliceThresholdBytes(originalRequestSliceThresholdBytes);
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
}
