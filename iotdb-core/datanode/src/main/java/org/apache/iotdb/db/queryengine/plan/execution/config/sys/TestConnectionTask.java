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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TServiceProvider;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class TestConnectionTask implements IConfigTask {

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.testConnection();
  }

  public static void buildTSBlock(
      TTestConnectionResp resp,
      int configNodeNum,
      int dataNodeNum,
      SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.testConnectionColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    sortTestConnectionResp(resp);
    int maxLen = calculateServiceProviderMaxLen(resp);
    for (TTestConnectionResult result : resp.getResultList()) {
      // ServiceProvider column
      builder.getTimeColumnBuilder().writeLong(0);
      StringBuilder serviceStr =
          new StringBuilder(serviceProviderToString(result.getServiceProvider()));
      while (serviceStr.length() < maxLen) {
        serviceStr.append(" ");
      }
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(serviceStr.toString(), TSFileConfig.STRING_CHARSET));
      // Sender column
      String senderStr;
      if (result.getSender().isSetConfigNodeLocation()) {
        senderStr =
            endPointToString(result.getSender().getConfigNodeLocation().getInternalEndPoint());
        senderStr += " (ConfigNode)";
      } else {
        senderStr =
            endPointToString(result.getSender().getDataNodeLocation().getInternalEndPoint());
        senderStr += " (DataNode)";
      }
      builder.getColumnBuilder(1).writeBinary(new Binary(senderStr, TSFileConfig.STRING_CHARSET));
      // Connection column
      String connectionStatus;
      if (result.isSuccess()) {
        connectionStatus = "up";
      } else {
        //        connectionStatus = addLineBreak("down" + " (" + result.getReason() + ")", 60);
        connectionStatus = "down" + " (" + result.getReason() + ")";
      }
      builder
          .getColumnBuilder(2)
          .writeBinary(new Binary(connectionStatus, TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getTestConnectionHeader()));
  }

  private static String serviceProviderToString(TServiceProvider provider) {
    String serviceStr = endPointToString(provider.getEndPoint());
    serviceStr += " (" + provider.getServiceType() + ")";
    return serviceStr;
  }

  private static String endPointToString(TEndPoint endPoint) {
    return endPoint.getIp() + ":" + endPoint.getPort();
  }

  private static void sortTestConnectionResp(TTestConnectionResp origin) {
    origin
        .getResultList()
        .sort(
            (o1, o2) -> {
              {
                String serviceIp1 = o1.getServiceProvider().getEndPoint().getIp();
                String serviceIp2 = o2.getServiceProvider().getEndPoint().getIp();
                if (!serviceIp1.equals(serviceIp2)) {
                  return serviceIp1.compareTo(serviceIp2);
                }
              }
              {
                int servicePort1 = o1.getServiceProvider().getEndPoint().getPort();
                int servicePort2 = o2.getServiceProvider().getEndPoint().getPort();
                if (servicePort1 != servicePort2) {
                  return Integer.compare(servicePort1, servicePort2);
                }
              }
              return 0;
            });
  }

  private static int calculateServiceProviderMaxLen(TTestConnectionResp origin) {
    return origin.getResultList().stream()
        .map(TTestConnectionResult::getServiceProvider)
        .map(TestConnectionTask::serviceProviderToString)
        .max(Comparator.comparingInt(String::length))
        .get()
        .length();
  }

  private static String addLineBreak(String origin, int interval) {
    if (origin.length() < interval) {
      return origin;
    }
    StringBuilder builder = new StringBuilder(origin.substring(0, interval));
    for (int i = interval; i < origin.length(); i += interval) {
      builder.append("\n");
      builder.append(origin, i, Math.min(origin.length(), i + interval));
    }
    return builder.toString();
  }
}
