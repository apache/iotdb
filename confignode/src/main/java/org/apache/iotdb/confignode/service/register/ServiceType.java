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
package org.apache.iotdb.confignode.service.register;

public enum ServiceType {
  JMX_SERVICE("JMX ServerService", "JMX ServerService"),
  CLUSTER_RPC_SERVICE("Cluster RPC Service", "ClusterRPCService");

  private final String name;
  private final String jmxName;

  ServiceType(String name, String jmxName) {
    this.name = name;
    this.jmxName = jmxName;
  }

  public String getName() {
    return name;
  }

  public String getJmxName() {
    return jmxName;
  }

  private static String generateJmxName(String packageName, String jmxName) {
    return String.format("%s:type=%s", packageName, jmxName);
  }
}
