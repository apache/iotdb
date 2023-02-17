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
      EnvType env = EnvType.getSystemEnvType();
      switch (env) {
        case Simple:
        case Cluster1:
          config = new MppConfig();
          break;
        case Remote:
          config = new RemoteServerConfig();
          break;
        default:
          System.out.println("Unknown env type: " + env);
          System.exit(-1);
          break;
      }
    }
    return config;
  }
}
