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
package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.*;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.concurrent.BlockingQueue;

public class DoubleWriteConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteConsumer.class);
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private TSIService.Iface doubleWriteClient;
  private TTransport transport;
  private long sessionId;
  private long consumerCnt = 0;
  private long consumerTime = 0;

  public DoubleWriteConsumer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue) {
    this.doubleWriteQueue = doubleWriteQueue;
    init();
  }

  @Override
  public void run() {
    try {
      while (true) {
        long startTime = System.nanoTime();
        Pair<DoubleWriteType, TSInsertRecordsReq> head = doubleWriteQueue.take();
        if (head.left == DoubleWriteType.DOUBLE_WRITE_END) {
          break;
        }
        switch (head.left) {
          case TSInsertRecordsReq:
            TSInsertRecordsReq tsInsertRecordsReq = head.right;
            try {
              RpcUtils.verifySuccessWithRedirection(
                  doubleWriteClient.insertRecords(tsInsertRecordsReq));
            } catch (TException e) {
              if (reconnect()) {
                try {
                  RpcUtils.verifySuccess(doubleWriteClient.insertRecords(tsInsertRecordsReq));
                } catch (TException tException) {
                  throw new IoTDBConnectionException(tException);
                }
              } else {
                throw new IoTDBConnectionException(
                    "Fail to reconnect to server. Please check server status");
              }
            }
            break;
        }
        consumerCnt += 1;
        long endTime = System.nanoTime();
        consumerTime += endTime - startTime;
      }

      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      try {
        doubleWriteClient.closeSession(req);
      } catch (TException e) {
        throw new IoTDBConnectionException(
            "Error occurs when closing session at server. Maybe server is down.", e);
      } finally {
        if (transport != null) {
          transport.close();
        }
      }
    } catch (RedirectException
        | StatementExecutionException
        | InterruptedException
        | IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  public double getEfficiency() {
    return (double) consumerCnt / (double) consumerTime * 1000000000.0;
  }

  private boolean reconnect() {
    boolean flag = false;
    for (int i = 1; i <= 3; i++) {
      try {
        if (transport != null) {
          close();
          init();
          flag = true;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          LOGGER.error("reconnect is interrupted.", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }

  private void close() throws IoTDBConnectionException {
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      doubleWriteClient.closeSession(req);
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  private void init() {
    try {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      RpcTransportFactory.setDefaultBufferCapacity(config.getThriftDefaultBufferSize());
      EndPoint endPoint = new EndPoint(config.getSecondaryAddress(), config.getSecondaryPort());
      RpcTransportFactory.setThriftMaxFrameSize(config.getThriftMaxFrameSize());

      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              new TSocket(endPoint.getIp(), endPoint.getPort(), 0));
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IoTDBConnectionException(e);
      }

      doubleWriteClient = new TSIService.Client(new TBinaryProtocol(transport));
      doubleWriteClient = RpcUtils.newSynchronizedClient(doubleWriteClient);

      TSOpenSessionReq openReq = new TSOpenSessionReq();
      openReq.setUsername(config.getSecondaryUser());
      openReq.setPassword(config.getSecondaryPassword());
      openReq.setZoneId(ZoneId.systemDefault().toString());

      try {
        TSOpenSessionResp openResp = doubleWriteClient.openSession(openReq);
        RpcUtils.verifySuccess(openResp.getStatus());
        sessionId = openResp.getSessionId();
      } catch (Exception e) {
        transport.close();
        throw new IoTDBConnectionException(e);
      }
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }
}
