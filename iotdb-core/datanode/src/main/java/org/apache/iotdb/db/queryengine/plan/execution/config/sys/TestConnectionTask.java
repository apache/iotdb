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
import org.apache.iotdb.common.rpc.thrift.TSender;
import org.apache.iotdb.common.rpc.thrift.TServiceProvider;
import org.apache.iotdb.common.rpc.thrift.TServiceType;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.ThriftService;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestConnectionTask implements IConfigTask {

  private final boolean needDetails;

  public TestConnectionTask(boolean needDetails) {
    this.needDetails = needDetails;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.testConnection(needDetails);
  }

  public static void buildTSBlock(
      TTestConnectionResp resp,
      int configNodeNum,
      int dataNodeNum,
      boolean needDetails,
      SettableFuture<ConfigTaskResult> future) {
    sortTestConnectionResp(resp);
    if (!needDetails) {
      Map<TServiceType, Integer> expectedNumMap =
          calculateExpectedResultNum(configNodeNum, dataNodeNum);
      List<TTestConnectionResult> newResultList = new ArrayList<>();
      for (int i = 0; i < resp.getResultListSize(); ) {
        TTestConnectionResult result = resp.getResultList().get(i);
        final int expectNum = expectedNumMap.get(result.getServiceProvider().getServiceType());
        final boolean allSameServiceProviderAllUp =
            resp.getResultList().stream()
                .skip(i)
                .limit(expectNum)
                .allMatch(
                    result1 ->
                        result.getServiceProvider().equals(result1.serviceProvider)
                            && result1.isSuccess());
        if (allSameServiceProviderAllUp) {
          TTestConnectionResult allUpResult = new TTestConnectionResult(result);
          allUpResult.setSender(new TSender());
          newResultList.add(allUpResult);
        } else {
          newResultList.addAll(
              resp.getResultList().stream()
                  .skip(i)
                  .limit(expectNum)
                  .filter(result1 -> !result1.isSuccess())
                  .collect(Collectors.toList()));
        }
        i += expectNum;
      }
      resp.setResultList(newResultList);
    }
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.testConnectionColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    int serviceProviderMaxLen = calculateServiceProviderMaxLen(resp);
    int connectionMaxLen = calculateConnectionMaxLen(resp);
    for (TTestConnectionResult result : resp.getResultList()) {
      // ServiceProvider column
      builder.getTimeColumnBuilder().writeLong(0);
      StringBuilder serviceStr =
          new StringBuilder(serviceProviderToString(result.getServiceProvider()));
      while (serviceStr.length() < serviceProviderMaxLen) {
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
      } else if (result.getSender().isSetDataNodeLocation()) {
        senderStr =
            endPointToString(result.getSender().getDataNodeLocation().getInternalEndPoint());
        senderStr += " (DataNode)";
      } else {
        senderStr = "All";
      }
      builder.getColumnBuilder(1).writeBinary(new Binary(senderStr, TSFileConfig.STRING_CHARSET));
      // Connection column
      StringBuilder connectionStatus = new StringBuilder(connectionResultToString(result));
      while (connectionStatus.length() < connectionMaxLen) {
        connectionStatus.append(" ");
      }
      builder
          .getColumnBuilder(2)
          .writeBinary(new Binary(connectionStatus.toString(), TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getTestConnectionHeader()));
  }

  private static Map<TServiceType, Integer> calculateExpectedResultNum(
      int configNodeNum, int dataNodeNum) {
    Map<TServiceType, Integer> result = new HashMap<>();
    result.put(TServiceType.ConfigNodeInternalService, configNodeNum + dataNodeNum);
    result.put(TServiceType.DataNodeInternalService, configNodeNum + dataNodeNum);
    result.put(TServiceType.DataNodeMPPService, dataNodeNum);
    result.put(TServiceType.DataNodeExternalService, dataNodeNum);
    return result;
  }

  private static String serviceProviderToString(TServiceProvider provider) {
    String serviceStr = endPointToString(provider.getEndPoint());
    serviceStr += " (" + provider.getServiceType() + ")";
    return serviceStr;
  }

  private static String connectionResultToString(TTestConnectionResult result) {
    if (result.isSuccess()) {
      return ThriftService.STATUS_UP;
    }
    return ThriftService.STATUS_DOWN + " (" + result.getReason() + ")";
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

  private static int calculateServiceProviderMaxLen(TTestConnectionResp resp) {
    return resp.getResultList().stream()
        .map(TTestConnectionResult::getServiceProvider)
        .map(TestConnectionTask::serviceProviderToString)
        .mapToInt(String::length)
        .max()
        .getAsInt();
  }

  private static int calculateConnectionMaxLen(TTestConnectionResp resp) {
    return resp.getResultList().stream()
        .map(TestConnectionTask::connectionResultToString)
        .mapToInt(String::length)
        .max()
        .getAsInt();
  }
}
