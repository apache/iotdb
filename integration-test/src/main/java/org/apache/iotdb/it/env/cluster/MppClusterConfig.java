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
package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.itbase.env.ClusterConfig;
import org.apache.iotdb.itbase.env.CommonConfig;
import org.apache.iotdb.itbase.env.ConfigNodeConfig;
import org.apache.iotdb.itbase.env.DataNodeConfig;
import org.apache.iotdb.itbase.env.JVMConfig;

/** MppClusterConfig stores a whole cluster config items. */
public class MppClusterConfig implements ClusterConfig {

  private final MppConfigNodeConfig configNodeConfig;
  private final MppDataNodeConfig dataNodeConfig;
  private final MppCommonConfig configNodeCommonConfig;
  private final MppCommonConfig dataNodeCommonConfig;
  private final MppSharedCommonConfig sharedCommonConfig;
  private final MppJVMConfig configNodeJVMConfig;
  private final MppJVMConfig dataNodeJVMConfig;

  public MppClusterConfig() {
    this.configNodeConfig = new MppConfigNodeConfig();
    this.dataNodeConfig = new MppDataNodeConfig();
    this.configNodeCommonConfig = new MppCommonConfig();
    this.dataNodeCommonConfig = new MppCommonConfig();
    this.sharedCommonConfig =
        new MppSharedCommonConfig(configNodeCommonConfig, dataNodeCommonConfig);
    this.configNodeJVMConfig = new MppJVMConfig();
    this.dataNodeJVMConfig = new MppJVMConfig();
  }

  @Override
  public DataNodeConfig getDataNodeConfig() {
    return dataNodeConfig;
  }

  @Override
  public CommonConfig getDataNodeCommonConfig() {
    return dataNodeCommonConfig;
  }

  @Override
  public ConfigNodeConfig getConfigNodeConfig() {
    return configNodeConfig;
  }

  @Override
  public CommonConfig getConfigNodeCommonConfig() {
    return configNodeCommonConfig;
  }

  @Override
  public CommonConfig getCommonConfig() {
    return sharedCommonConfig;
  }

  @Override
  public JVMConfig getConfigNodeJVMConfig() {
    return configNodeJVMConfig;
  }

  @Override
  public JVMConfig getDataNodeJVMConfig() {
    return dataNodeJVMConfig;
  }
}
