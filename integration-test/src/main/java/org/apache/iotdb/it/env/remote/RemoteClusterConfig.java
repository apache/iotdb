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
package org.apache.iotdb.it.env.remote;

import org.apache.iotdb.itbase.env.ClusterConfig;
import org.apache.iotdb.itbase.env.CommonConfig;
import org.apache.iotdb.itbase.env.ConfigNodeConfig;
import org.apache.iotdb.itbase.env.DataNodeConfig;
import org.apache.iotdb.itbase.env.JVMConfig;

public class RemoteClusterConfig implements ClusterConfig {

  private final CommonConfig commonConfig = new RemoteCommonConfig();
  private final ConfigNodeConfig configNodeConfig = new RemoteConfigNodeConfig();
  private final DataNodeConfig dataNodeConfig = new RemoteDataNodeConfig();
  private final RemoteJVMConfig jvmConfig = new RemoteJVMConfig();

  @Override
  public DataNodeConfig getDataNodeConfig() {
    return dataNodeConfig;
  }

  @Override
  public CommonConfig getDataNodeCommonConfig() {
    return commonConfig;
  }

  @Override
  public ConfigNodeConfig getConfigNodeConfig() {
    return configNodeConfig;
  }

  @Override
  public CommonConfig getConfigNodeCommonConfig() {
    return commonConfig;
  }

  @Override
  public CommonConfig getCommonConfig() {
    return commonConfig;
  }

  @Override
  public JVMConfig getConfigNodeJVMConfig() {
    return jvmConfig;
  }

  @Override
  public JVMConfig getDataNodeJVMConfig() {
    return jvmConfig;
  }
}
