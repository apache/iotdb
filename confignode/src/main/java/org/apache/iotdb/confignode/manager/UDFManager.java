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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableFetcher;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.persistence.UDFInfo;

import java.io.IOException;
import java.util.List;

public class UDFManager {

  private static final ConfigNodeConf CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final UDFExecutableFetcher udfExecutableFetcher;
  private final UDFClassLoaderManager udfClassLoaderManager;
  private final UDFRegistrationService udfRegistrationService;

  private final ConfigManager configManager;
  private final UDFInfo udfInfo;

  public UDFManager(ConfigManager configManager, UDFInfo udfInfo) throws IOException {
    this.configManager = configManager;
    this.udfInfo = udfInfo;

    udfExecutableFetcher =
        UDFExecutableFetcher.setupAndGetInstance(
            CONFIG_NODE_CONF.getTemporaryLibDir(), CONFIG_NODE_CONF.getUdfLibDir());
    udfClassLoaderManager =
        UDFClassLoaderManager.setupAndGetInstance(CONFIG_NODE_CONF.getUdfLibDir());
    udfRegistrationService =
        UDFRegistrationService.setupAndGetInstance(CONFIG_NODE_CONF.getSystemUdfDir());
  }

  public TSStatus createFunction(String udfName, String className, List<String> uris) {
    return new TSStatus();
  }
}
