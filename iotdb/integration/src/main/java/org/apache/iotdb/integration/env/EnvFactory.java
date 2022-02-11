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
package org.apache.iotdb.integration.env;

import org.apache.iotdb.itbase.env.BaseEnv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvFactory {
  private static BaseEnv env;
  private static final Logger logger = LoggerFactory.getLogger(EnvFactory.class);

  public static BaseEnv getEnv() {
    if (env == null) {
      try {
        logger.debug(">>>>>>>" + System.getProperty("TestEnv"));
        switch (System.getProperty("TestEnv", "Standalone")) {
          case "Standalone":
            env =
                (BaseEnv)
                    Class.forName("org.apache.iotdb.db.integration.env.StandaloneEnv")
                        .newInstance();
            break;
          case "Remote":
            env = new RemoteServerEnv();
            break;
          case "FiveNodeCluster1":
            env = new FiveNodeCluster1Env();
            break;
          default:
            throw new ClassNotFoundException("The Property class of TestEnv not found");
        }
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
    return env;
  }
}
