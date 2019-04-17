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

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.cluster.utils.hash.VirtualNode;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMonitor implements ClusterMonitorMBean, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMonitor.class);

  public static final ClusterMonitor INSTANCE = new ClusterMonitor();
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private Router router = Router.getInstance();

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      LOGGER.error(errorMessage);
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_MONITOR_SERVICE;
  }

  @Override
  public SortedMap<Integer, PhysicalNode> getPhysicalRing() {
    return router.getPhysicalRing();
  }

  @Override
  public SortedMap<Integer, VirtualNode> getVirtualRing() {
    return router.getVirtualRing();
  }

  @Override
  public Map<String, String> getAllLeaders() {
    Map<String, String> map = new HashMap<>();
    RaftUtils.getGroupLeaderCache().entrySet().forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
    return map;
  }

  @Override
  public Map<String, String[]> getAllGroups() {
    return null;
  }

  @Override
  public String getLeaderOfSG(String sg) {
    PhysicalNode[] group = router.routeGroup(sg);
    String groupId = router.getGroupID(group);
    PeerId leader = RaftUtils.getLeaderPeerID(groupId);
    return leader.toString();
  }
}
