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

package org.apache.iotdb.db.sql.nodes5;

// read and the write statements are on the different nodes, and maybe in the different raft groups.
public class FiveNodeCluster4IT extends AbstractFiveNodeClusterIT {

  protected String getWriteRpcIp() {
    return getContainer().getServiceHost("iotdb-server_4", 6667);
  }

  protected int getWriteRpcPort() {
    return getContainer().getServicePort("iotdb-server_4", 6667);
  }
}
