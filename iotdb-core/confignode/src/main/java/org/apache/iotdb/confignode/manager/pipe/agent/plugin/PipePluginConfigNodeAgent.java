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

package org.apache.iotdb.confignode.manager.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.agent.plugin.PipePluginAgent;
import org.apache.iotdb.commons.pipe.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipePluginConfigNodeAgent extends PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginConfigNodeAgent.class);

  protected ConfigNodePipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipeConfigRegionExtractorConstructor pipeExtractorConstructor;
  private final PipeConfigRegionConnectorConstructor pipeConnectorConstructor;

  public PipePluginConfigNodeAgent() {
    this.pipePluginMetaKeeper = new ConfigNodePipePluginMetaKeeper();
    this.pipeExtractorConstructor = new PipeConfigRegionExtractorConstructor(pipePluginMetaKeeper);
    this.pipeConnectorConstructor = new PipeConfigRegionConnectorConstructor(pipePluginMetaKeeper);
  }

  @Override
  public PipeExtractor reflectExtractor(PipeParameters extractorParameters) {
    return pipeExtractorConstructor.reflectPlugin(extractorParameters);
  }

  @Override
  public PipeProcessor reflectProcessor(PipeParameters processorParameters) {
    throw new UnsupportedOperationException(
        "Schema pipes do not support processors on config node.");
  }

  @Override
  public PipeConnector reflectConnector(PipeParameters connectorParameters) {
    return pipeConnectorConstructor.reflectPlugin(connectorParameters);
  }
}
