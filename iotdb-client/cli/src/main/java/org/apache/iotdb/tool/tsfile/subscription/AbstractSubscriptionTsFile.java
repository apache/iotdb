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

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.tool.common.Constants;

import java.util.concurrent.ExecutorService;

public abstract class AbstractSubscriptionTsFile {

  protected static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  protected static CommonParam commonParam = CommonParam.getInstance();

  public static void setSubscriptionSession() throws IoTDBConnectionException {
    if (Constants.TABLE_MODEL.equalsIgnoreCase(commonParam.getSqlDialect())) {
      commonParam.setSubscriptionTsFile(new SubscriptionTableTsFile());
      commonParam.setTableSubs(
          new SubscriptionTableSessionBuilder()
              .host(commonParam.getSrcHost())
              .port(commonParam.getSrcPort())
              .username(commonParam.getSrcUserName())
              .password(commonParam.getSrcPassword())
              .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
              .build());
      commonParam.getTableSubs().open();
    } else {

      commonParam.setSubscriptionTsFile(new SubscriptionTreeTsFile());
      commonParam.setTreeSubs(
          new SubscriptionTreeSessionBuilder()
              .host(commonParam.getSrcHost())
              .port(commonParam.getSrcPort())
              .username(commonParam.getSrcUserName())
              .password(commonParam.getSrcPassword())
              .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
              .build());
      commonParam.getTreeSubs().open();
    }
  }

  public abstract void createTopics(String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void doClean() throws Exception;

  public abstract void createConsumers(String groupId);

  public abstract void subscribe(String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void consumerPoll(ExecutorService executor, String topicName);
}
