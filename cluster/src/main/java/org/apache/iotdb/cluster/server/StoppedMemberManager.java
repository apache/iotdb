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

package org.apache.iotdb.cluster.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a node is added or removed, several partition groups are affected and nodes may exit some
 * groups. For example, the local node is #5 and it is in a data group of [1, 3, 5], then node #3
 * is added, so the group becomes [1, 3, 4] and the local node must leave the group. However, #5
 * may have data that #4 needs to pull, so the Member of #5 in this group is stopped but not
 * removed yet and when system recovers, we need to resume the groups so that they can keep
 * providing snapshots for data transfers.
 */
public class StoppedMemberManager {

  private static final Logger logger = LoggerFactory.getLogger(StoppedMemberManager.class);
  private String stoppedMembersFileName =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "removedMembers";

  private static final String REMOVED = "0";
  private static final String RESUMED = "1";

  private Map<Node, DataGroupMember> removedMemberMap = new HashMap<>();
  private DataGroupMember.Factory memberFactory;
  private Node thisNode;

  StoppedMemberManager(
      Factory memberFactory, Node thisNode) {
    this.memberFactory = memberFactory;
    this.thisNode = thisNode;
    recover();
  }

  /**
   * When a DataGroupMember is removed, add it here and record this removal, so in next start-up
   * we can recover it as a data source for data transfers.
   * @param header
   * @param dataGroupMember
   */
  public synchronized void put(Node header, DataGroupMember dataGroupMember) {
    removedMemberMap.put(header, dataGroupMember);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stoppedMembersFileName, true))) {
      StringBuilder builder = new StringBuilder(REMOVED);
      for (Node node : dataGroupMember.getAllNodes()) {
        builder.append(";").append(node.toString());
      }
      writer.write(builder.toString());
      writer.newLine();
    } catch (IOException e) {
      logger.error("Cannot record removed member of header {}", header, e);
    }
  }

  /**
   * When a DataGroupMember is resumed, add it here and record this removal, so in next start-up
   * we will not recover it here.
   * @param header
   */
  public synchronized void remove(Node header) {
    removedMemberMap.remove(header);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stoppedMembersFileName, true))) {
      writer.write(RESUMED + ";" + header.toString());
      writer.newLine();
    } catch (IOException e) {
      logger.error("Cannot record resumed member of header {}", header, e);
    }
  }

  public synchronized DataGroupMember get(Node header) {
    return removedMemberMap.get(header);
  }

  private void recover() {
    File stoppedMembersFile = new File(stoppedMembersFileName);
    if (!stoppedMembersFile.exists()) {
      return;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(stoppedMembersFileName))) {
      String line;
      while ((line = reader.readLine()) != null) {
        parseLine(line);
      }
    } catch (IOException e) {
      logger.error("Cannot recover members from file", e);
    }
  }

  private void parseLine(String line) {
    if ("".equalsIgnoreCase(line)) {
      return;
    }
    try {
      String[] split = line.split(";");
      String type = split[0];
      if (REMOVED.equalsIgnoreCase(type)) {
        parseRemoved(split);
      } else if (RESUMED.equalsIgnoreCase(type)) {
        parseResumed(split);
      }
    } catch (Exception e) {
      logger.warn("Fail to analyze {}, skipping", line);
    }
  }

  private void parseRemoved(String[] split) {
    PartitionGroup partitionGroup = new PartitionGroup();
    for (int i = 1; i < split.length; i++) {
      Node node = ClusterUtils.stringToNode(split[i]);
      partitionGroup.add(node);
    }
    DataGroupMember member = memberFactory.create(partitionGroup, thisNode);
    member.setReadOnly();
    removedMemberMap.put(partitionGroup.getHeader(), member);
  }

  private void parseResumed(String[] split) {
    Node header = ClusterUtils.stringToNode(split[1]);
    removedMemberMap.remove(header);
  }
}
