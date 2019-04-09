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


import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBMetadataFetchLocallyIT extends IoTDBMetadataFetchAbstract{

  private Server server;
  private ClusterConfig config = ClusterDescriptor.getInstance().getConfig();

  private final String URL = "127.0.0.1:6667/";
  private Map<String, String> testConfigMap = new HashMap<String, String>() {
    private static final long serialVersionUID = 7832408957178621132L;
    {

      put("port", "8888");
      put("election_timeout_ms", "1000");
      put("max_catch_up_log_num", "100000");
      put("delay_snapshot", "false");
      put("delay_hours", "2");
      put("task_redo_count", "3");
      put("task_timeout_ms", "1000");
      put("num_of_virtual_nodes", "2");
      put("max_num_of_inner_rpc_client", "50");
      put("max_queue_num_of_inner_rpc_client", "50");
      put("read_metadata_consistency_level", "1");
      put("read_data_consistency_level", "1");
    }
  };
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    config.createAllPath();
    server = Server.getInstance();
    server.start();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testLocal() throws SQLException {
    test(URL, false);
  }

  @Test
  public void testBatchLocal() throws SQLException {
    test(URL, true);
  }






}
