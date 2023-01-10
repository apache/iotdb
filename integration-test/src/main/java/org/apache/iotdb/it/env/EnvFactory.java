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

import org.apache.iotdb.it.env.cluster.Cluster1Env;
import org.apache.iotdb.it.env.cluster.SimpleEnv;
import org.apache.iotdb.it.env.remote.RemoteServerEnv;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;

import org.slf4j.Logger;

public class EnvFactory {
  private static BaseEnv env;
  private static final Logger logger = IoTDBTestLogger.logger;

  public static BaseEnv getEnv() {
    if (env == null) {
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        logger.debug(">>>>>>>" + System.getProperty("TestEnv"));
        EnvType envType = EnvType.getSystemEnvType();
        switch (envType) {
          case Simple:
            env = new SimpleEnv();
            break;
          case Cluster1:
            env = new Cluster1Env();
            break;
          case Remote:
            env = new RemoteServerEnv();
            break;
          default:
            System.out.println("Unknown env type: " + envType);
            System.exit(-1);
            break;
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
    return env;
  }
}
