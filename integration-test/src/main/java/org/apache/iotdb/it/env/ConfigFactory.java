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

import org.apache.iotdb.itbase.env.BaseConfig;

public class ConfigFactory {
  private static BaseConfig config;

  public static BaseConfig getConfig() {
    if (config == null) {
      try {
        switch (System.getProperty("TestEnv", "Standalone")) {
          case "Standalone":
            config =
                (BaseConfig)
                    Class.forName("org.apache.iotdb.db.integration.env.StandaloneEnvConfig")
                        .newInstance();
            break;
          case "Remote":
            config = new RemoteEnvConfig();
            break;
          case "Cluster1":
            config = new ClusterEnvConfig();
            break;
          default:
            throw new ClassNotFoundException("The Property class of TestEnv not found");
        }
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
    return config;
  }
}
