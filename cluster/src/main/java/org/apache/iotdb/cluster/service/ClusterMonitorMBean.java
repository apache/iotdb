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
package org.apache.iotdb.cluster.service;

import java.util.Map;

public interface ClusterMonitorMBean {

  /**
   * Get physical hash ring
   *
   * @return key: hash value, value: node ip
   */
  Map<Integer, String> getPhysicalRing();

  /**
   * Get virtual hash ring
   *
   * @return key: hash value, value: node ip
   */
  Map<Integer, String> getVirtualRing();

  /**
   * Get currents leaders of each data partition
   *
   * @return key: group id, value: leader node ip
   */
  Map<String, String> getAllLeaders();

  /**
   * Get data partition information of input storage group in String format. The node ips are split
   * by ',', and the first ip is the currnt leader.
   *
   * @param sg input storage group path
   * @return data partition information in String format
   */
  String getDataPartitionOfSG(String sg);

  /**
   * Get data partitions that input node belongs to.
   *
   * @param ip node ip
   * @param port node rpc port
   * @return key: node ips of one data partition, value: storage group paths that belong to this
   * data partition
   */
  Map<String[], String[]> getDataPartitonOfNode(String ip, int port);
  Map<String[], String[]> getDataPartitonOfNode(String ip);

  /**
   * Get replica lag for metadata group and each data partition
   *
   * @return key: groupId, value: ip -> replica lag
   */
  Map<String, Map<String, Long>> getReplicaLagMap();

  /**
   * Get number of query jobs on each data partition for all nodes
   *
   * @return outer key: ip, inner key: groupId, value: number of query jobs
   */
  Map<String, Map<String, Integer>> getQueryJobNumMap();
}
