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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.exception.BadNodeUrlException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IoTDBStopCheck {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBStopCheck.class);

  public static IoTDBStopCheck getInstance() {
    return IoTDBStopCheckHolder.INSTANCE;
  }

  private static class IoTDBStopCheckHolder {
    private static final IoTDBStopCheck INSTANCE = new IoTDBStopCheck();
  }

  /**
   * check datanode ips is duplicate or note
   *
   * @param dataNodeIps Data Node IP list
   * @throws BadNodeUrlException check failed
   */
  public void checkDuplicateIp(List<String> dataNodeIps) throws BadNodeUrlException {
    if (dataNodeIps == null || dataNodeIps.isEmpty()) {
      throw new BadNodeUrlException("Data Node ips is empty");
    }
    long realIpNumber = dataNodeIps.stream().distinct().count();
    if (realIpNumber != dataNodeIps.size()) {
      throw new BadNodeUrlException("has replicate ips");
    }
  }

  /**
   * check the remove Data Node ips is in cluster or not
   *
   * @param removedDataNodeIps removed Data Node Ip list
   * @param onlineDataNodeIps all online Data Node Ip list
   * @throws BadNodeUrlException check failed
   */
  public void checkIpInCluster(List<String> removedDataNodeIps, List<String> onlineDataNodeIps)
      throws BadNodeUrlException {
    if (removedDataNodeIps == null || removedDataNodeIps.isEmpty()) {
      throw new BadNodeUrlException("checked Data Node ips is empty");
    }

    if (onlineDataNodeIps == null || onlineDataNodeIps.isEmpty()) {
      throw new BadNodeUrlException("online Data Node ips is empty");
    }

    if (removedDataNodeIps.stream().anyMatch(ip -> !onlineDataNodeIps.contains(ip))) {
      throw new BadNodeUrlException("exist Data Node not in cluster");
    }
  }
}
