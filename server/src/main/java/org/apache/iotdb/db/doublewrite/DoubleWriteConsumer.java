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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.BlockingQueue;

public class DoubleWriteConsumer implements Runnable {
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private TSIService.Iface doubleWriteClient;
  private TTransport transport;
  private long sessionId;
  private long consumerCnt = 0;
  private long consumerTime = 0;

  public DoubleWriteConsumer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue,
      TSIService.Iface doubleWriteClient,
      TTransport transport,
      long sessionId) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.doubleWriteClient = doubleWriteClient;
    this.transport = transport;
    this.sessionId = sessionId;
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
            doubleWriteClient.insertRecords(tsInsertRecordsReq);
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
    } catch (TException | InterruptedException | IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  public double getEfficiency() {
    return (double) consumerCnt / (double) consumerTime * 1000000000.0;
  }
}
