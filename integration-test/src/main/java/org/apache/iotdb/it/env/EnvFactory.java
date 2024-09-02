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

package org.apache.iotdb.it.env;

import org.apache.iotdb.it.env.cluster.env.AIEnv;
import org.apache.iotdb.it.env.cluster.env.Cluster1Env;
import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.env.remote.env.RemoteServerEnv;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;

import org.slf4j.Logger;

public class EnvFactory {
  private static BaseEnv env;
  private static final Logger logger = IoTDBTestLogger.logger;

  private EnvFactory() {
    // Empty constructor
  }

  public static BaseEnv getEnv() {
    if (env == null) {
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        logger.debug(">>>>>>>{}", System.getProperty("TestEnv"));
        EnvType envType = EnvType.getSystemEnvType();
        switch (envType) {
          case Simple:
          case TABLE_SIMPLE:
            env = new SimpleEnv();
            break;
          case Cluster1:
          case TABLE_CLUSTER1:
            env = new Cluster1Env();
            break;
          case Remote:
            env = new RemoteServerEnv();
            break;
          case AI:
            env = new AIEnv();
            break;
          case MultiCluster:
            logger.warn(
                "EnvFactory only supports EnvType Simple, Cluster1 and Remote, please use MultiEnvFactory instead.");
            System.exit(-1);
            break;
          default:
            logger.warn("Unknown env type: {}", envType);
            System.exit(-1);
            break;
        }
      } catch (ClassNotFoundException e) {
        logger.error("Get env error", e);
        System.exit(-1);
      }
    }
    return env;
  }
}
