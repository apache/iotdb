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
package org.apache.iotdb.itbase.env;

public interface ClusterConfig {

  /**
   * Get the iotdb-datanode.properties setter of DataNodes.
   *
   * @return the {@link DataNodeConfig} instance.
   */
  DataNodeConfig getDataNodeConfig();

  /**
   * Get the iotdb-common.properties setter of DataNodes. Updating the instance will affect
   * ConfigNodes only.
   *
   * @return the {@link CommonConfig} instance.
   */
  CommonConfig getDataNodeCommonConfig();

  /**
   * Get the iotdb-confignode.properties setter of ConfigNodes.
   *
   * @return the {@link ConfigNodeConfig} instance.
   */
  ConfigNodeConfig getConfigNodeConfig();

  /**
   * Get the iotdb-common.properties setter of ConfigNodes. Updating the instance will affect
   * ConfigNodes only.
   *
   * @return the {@link CommonConfig} instance.
   */
  CommonConfig getConfigNodeCommonConfig();

  /**
   * Get the iotdb-common.properties setter for both the ConfigNodes and DataNodes. Updating the
   * instance will affect ConfigNodes as well as DataNodes.
   *
   * @return the {@link CommonConfig} instance.
   */
  CommonConfig getCommonConfig();

  /**
   * Get the JVM Configuration setter of ConfigNodes.
   *
   * @return the {@link JVMConfig} instance.
   */
  JVMConfig getConfigNodeJVMConfig();

  /**
   * Get the JVM Configuration setter of DataNodes.
   *
   * @return the {@link JVMConfig} instance.
   */
  JVMConfig getDataNodeJVMConfig();
}
