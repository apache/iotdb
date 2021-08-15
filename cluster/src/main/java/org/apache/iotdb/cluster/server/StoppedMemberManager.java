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

import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * When a node is added or removed, several partition groups are affected and nodes may exit some
 * groups. For example, the local node is #5 and it is in a data group of [1, 3, 5], then node #3 is
 * added, so the group becomes [1, 3, 4] and the local node must leave the group. However, #5 may
 * have data that #4 needs to pull, so the Member of #5 in this group is stopped but not removed yet
 * and when system recovers, we need to resume the groups so that they can keep providing snapshots
 * for data transfers.
 */
public class StoppedMemberManager {

  private static final Logger logger = LoggerFactory.getLogger(StoppedMemberManager.class);
  private String stoppedMembersFileName =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "removedMembers";

  private static final String REMOVED = "0";
  private static final String RESUMED = "1";

  private Map<RaftNode, DataGroupMember> removedMemberMap = new HashMap<>();
  private DataGroupMember.Factory memberFactory;
  private Node thisNode;

  StoppedMemberManager(Factory memberFactory, Node thisNode) {
    this.memberFactory = memberFactory;
    this.thisNode = thisNode;
    recover();
  }

  /**
   * When a DataGroupMember is removed, add it here and record this removal, so in next start-up we
   * can recover it as a data source for data transfers.
   *
   * @param header When a DataGroupMember is removed, add it here and record this removal, so in
   *     next start-up we can recover it as a data source for data transfers.
   * @param raftNode
   * @param dataGroupMember
   */
  public synchronized void put(RaftNode raftNode, DataGroupMember dataGroupMember) {
    removedMemberMap.put(raftNode, dataGroupMember);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stoppedMembersFileName, true))) {
      StringBuilder builder = new StringBuilder(REMOVED);
      builder.append(";").append(raftNode.raftId);
      for (Node node : dataGroupMember.getAllNodes()) {
        builder.append(";").append(node.toString());
      }
      writer.write(builder.toString());
      writer.newLine();
    } catch (IOException e) {
      logger.error("Cannot record removed member of header {}", raftNode, e);
    }
  }

  /**
   * When a DataGroupMember is resumed, add it here and record this removal, so in next start-up we
   * will not recover it here.
   *
   * @param raftNode
   */
  public synchronized void remove(RaftNode raftNode) {
    removedMemberMap.remove(raftNode);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stoppedMembersFileName, true))) {
      writer.write(RESUMED + ";" + raftNode.getRaftId() + ";" + raftNode.getNode().toString());
      writer.newLine();
    } catch (IOException e) {
      logger.error("Cannot record resumed member of header {}", raftNode, e);
    }
  }

  public synchronized DataGroupMember get(RaftNode raftNode) {
    return removedMemberMap.get(raftNode);
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
    int raftId = Integer.parseInt(split[1]);
    partitionGroup.setId(raftId);
    for (int i = 2; i < split.length; i++) {
      Node node = ClusterUtils.stringToNode(split[i]);
      partitionGroup.add(node);
    }
    DataGroupMember member = memberFactory.create(partitionGroup, thisNode);
    member.setReadOnly();
    removedMemberMap.put(partitionGroup.getHeader(), member);
  }

  private void parseResumed(String[] split) {
    int raftId = Integer.parseInt(split[1]);
    Node header = ClusterUtils.stringToNode(split[2]);
    removedMemberMap.remove(new RaftNode(header, raftId));
  }
}
