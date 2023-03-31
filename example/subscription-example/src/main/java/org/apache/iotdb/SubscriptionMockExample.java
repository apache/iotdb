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

package org.apache.iotdb;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.subscription.rpc.thrift.ISubscriptionRPCService;
import org.apache.iotdb.subscription.rpc.thrift.TSubscriptionDataSet;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SubscriptionMockExample {
  public static void main(String[] args) throws TException, InterruptedException {
    TTransport transport;
    try {
      //      RpcTransportFactory.INSTANCE.setDefaultBufferCapacity(1024);
      transport = RpcTransportFactory.INSTANCE.getTransport("localhost", 9997, 1000);
      transport = new TSocket("localhost", 9997);
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new TTransportException("Failed to open transport", e);
    }
    ISubscriptionRPCService.Iface client =
        new ISubscriptionRPCService.Client(new TBinaryProtocol(transport));
    for (int i = 0; i < 100; i++) {

      try {
        client.pushSubscriptionData(generateMockData());
      } catch (TException e) {
        System.out.println(e);
      }
    }
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.MINUTES);
    transport.close();
  }

  public static List<TSubscriptionDataSet> generateMockData() {
    List<TSubscriptionDataSet> result = new ArrayList<>();
    Integer randomSize = new Random().nextInt(10);
    for (int i = 0; i < randomSize; i++) {
      TSubscriptionDataSet dataSet = new TSubscriptionDataSet();
      dataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      Long nextTime = System.currentTimeMillis();
      // 随机是否乱序
      if (new Random().nextBoolean()) {
        dataSet.setTime(nextTime);
      } else {
        dataSet.setTime(nextTime - new Random().nextInt(100));
      }
      dataSet.setColumnNames(new ArrayList<>());
      dataSet.setColumnTypes(new ArrayList<>());
      dataSet.setDataResult(new ArrayList<>());
      Integer randomSize2 = new Random().nextInt(10);
      for (int j = 0; j < randomSize2; j++) {
        dataSet.getColumnNames().add("root.sg1.d1.t" + new Random().nextInt(100));
        dataSet.getColumnTypes().add("INT32");
        ByteBuffer valueBuffer = ByteBuffer.allocate(4);
        valueBuffer.putInt(new Random().nextInt(100));
        valueBuffer.flip();
        dataSet.getDataResult().add(valueBuffer);
      }
      result.add(dataSet);
    }
    return result;
  }
}
