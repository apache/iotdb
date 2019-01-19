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
package org.apache.iotdb.db.service;

public enum ServiceType {
  FILE_NODE_SERVICE("File Node ServerService", ""), JMX_SERVICE("JMX ServerService",
      "JMX ServerService"), JDBC_SERVICE("JDBC ServerService", "JDBCService"), MONITOR_SERVICE(
      "Monitor ServerService", "Monitor"), STAT_MONITOR_SERVICE("Statistics ServerService",
      ""), WAL_SERVICE("WAL ServerService", ""), CLOSE_MERGE_SERVICE("Close&Merge ServerService",
      ""), JVM_MEM_CONTROL_SERVICE("Memory Controller", ""), AUTHORIZATION_SERVICE(
      "Authorization ServerService",
      ""), FILE_READER_MANAGER_SERVICE("File reader manager ServerService", "");
  private String name;
  private String jmxName;

  private ServiceType(String name, String jmxName) {
    this.name = name;
    this.jmxName = jmxName;
  }

  public String getName() {
    return name;
  }

  public String getJmxName() {
    return jmxName;
  }
}
