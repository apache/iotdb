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

package org.apache.iotdb.tool.tsfile.subscription;

import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonParam {

  private static AtomicInteger countFile = new AtomicInteger(0);
  private static String path = "root.**";
  private static String table = ".*";
  private static String database = ".*";
  private static int startIndex = 0;
  private static int consumerCount = 8;
  private static ISubscriptionTableSession tableSubs;
  private static ISubscriptionTreeSession treeSubs;

  private static String pathFull = "root.**";
  private static String srcHost = "127.0.0.1";
  private static int srcPort = 6667;
  private static String srcUserName = "root";
  private static String srcPassword = "root";
  private static String sqlDialect = "tree";
  private static String startTime = "";
  private static String endTime = "";
  private static String targetDir = "target";
  private static List<ISubscriptionTablePullConsumer> pullTableConsumers;
  private static List<SubscriptionTreePullConsumer> pullTreeConsumers;

  private static AbstractSubscriptionTsFile subscriptionTsFile;

  private static CommonParam instance;

  public static synchronized CommonParam getInstance() {
    if (null == instance) {
      instance = new CommonParam();
    }
    return instance;
  }

  public static String getPath() {
    return path;
  }

  public static void setPath(String path) {
    CommonParam.path = path;
  }

  public static String getTable() {
    return table;
  }

  public static void setTable(String table) {
    CommonParam.table = table;
  }

  public static String getDatabase() {
    return database;
  }

  public static void setDatabase(String database) {
    CommonParam.database = database;
  }

  public static int getStartIndex() {
    return startIndex;
  }

  public static int getConsumerCount() {
    return consumerCount;
  }

  public static void setConsumerCount(int consumerCount) {
    CommonParam.consumerCount = consumerCount;
  }

  public static ISubscriptionTableSession getTableSubs() {
    return tableSubs;
  }

  public static void setTableSubs(ISubscriptionTableSession tableSubs) {
    CommonParam.tableSubs = tableSubs;
  }

  public static ISubscriptionTreeSession getTreeSubs() {
    return treeSubs;
  }

  public static void setTreeSubs(ISubscriptionTreeSession treeSubs) {
    CommonParam.treeSubs = treeSubs;
  }

  public static String getPathFull() {
    return pathFull;
  }

  public static String getSrcHost() {
    return srcHost;
  }

  public static void setSrcHost(String srcHost) {
    CommonParam.srcHost = srcHost;
  }

  public static int getSrcPort() {
    return srcPort;
  }

  public static void setSrcPort(int srcPort) {
    CommonParam.srcPort = srcPort;
  }

  public static String getSrcUserName() {
    return srcUserName;
  }

  public static void setSrcUserName(String srcUserName) {
    CommonParam.srcUserName = srcUserName;
  }

  public static String getSrcPassword() {
    return srcPassword;
  }

  public static void setSrcPassword(String srcPassword) {
    CommonParam.srcPassword = srcPassword;
  }

  public static String getSqlDialect() {
    return sqlDialect;
  }

  public static void setSqlDialect(String sqlDialect) {
    CommonParam.sqlDialect = sqlDialect;
  }

  public static String getStartTime() {
    return startTime;
  }

  public static void setStartTime(String startTime) {
    CommonParam.startTime = startTime;
  }

  public static String getEndTime() {
    return endTime;
  }

  public static void setEndTime(String endTime) {
    CommonParam.endTime = endTime;
  }

  public static String getTargetDir() {
    return targetDir;
  }

  public static void setTargetDir(String targetDir) {
    CommonParam.targetDir = targetDir;
  }

  public static List<ISubscriptionTablePullConsumer> getPullTableConsumers() {
    return pullTableConsumers;
  }

  public static void setPullTableConsumers(
      List<ISubscriptionTablePullConsumer> pullTableConsumers) {
    CommonParam.pullTableConsumers = pullTableConsumers;
  }

  public static List<SubscriptionTreePullConsumer> getPullTreeConsumers() {
    return pullTreeConsumers;
  }

  public static void setPullTreeConsumers(List<SubscriptionTreePullConsumer> pullTreeConsumers) {
    CommonParam.pullTreeConsumers = pullTreeConsumers;
  }

  public static AbstractSubscriptionTsFile getSubscriptionTsFile() {
    return subscriptionTsFile;
  }

  public static void setSubscriptionTsFile(AbstractSubscriptionTsFile subscriptionTsFile) {
    CommonParam.subscriptionTsFile = subscriptionTsFile;
  }

  public static AtomicInteger getCountFile() {
    return countFile;
  }
}
