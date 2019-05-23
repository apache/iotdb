/**
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
package org.apache.iotdb.cluster.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.iotdb.cluster.utils.ClusterConfigureGenerator;
import org.apache.iotdb.cluster.utils.Utils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBMetadataFetchRemoteIT extends IoTDBMetadataFetchAbstract {

  @BeforeClass
  public static void setUp() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    ClusterConfigureGenerator.generateClusterConfigure();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ClusterConfigureGenerator.deleteClusterConfigure();
  }

  @Test
  public void test() throws IOException {
//    String dir = Utils.getCurrentPath("pwd");
//    String node = "3";
//    String replicator = "3";
//    startScript("sh", dir + File.separator + "script" + File.separator + "deploy.sh", node, replicator, dir);
//    startScript("sh", dir + File.separator + "script" + File.separator + "stop.sh", node, replicator, dir);
  }

  private void startScript(String... command) throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while (true) {
      line = r.readLine();
      if (line == null) {
        break;
      } else {
        System.out.println(line);
      }
    }
    r.close();
    p.destroy();
  }
}
