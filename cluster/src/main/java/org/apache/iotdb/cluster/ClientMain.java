/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client.Factory;
import org.apache.iotdb.service.rpc.thrift.TSInsertReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ClientMain {

  public static void main(String[] args) throws TException, InterruptedException {
    String ip = "127.0.0.1";
    int port = 55560;
    TSIService.Client.Factory factory = new Factory();
    TTransport transport = new TFramedTransport(new TSocket(ip, port));
    transport.open();

    Client client = factory.getClient(new TCompactProtocol(transport));

    TSOpenSessionReq openReq = new TSOpenSessionReq(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1);

    openReq.setUsername("root");
    openReq.setPassword("root");
    TSOpenSessionResp openResp = client.openSession(openReq);
    TS_SessionHandle sessionHandle = openResp.getSessionHandle();

    //testInsertion(client);

    testQuery(client, sessionHandle);

    client.closeSession(new TSCloseSessionReq(openResp.getSessionHandle()));
  }

  private static void testQuery(Client client, TS_SessionHandle handle) throws TException {
    long statementId = client.requestStatementId();
    String statement = "SELECT d1.s1 FROM root.shenzhen";
    TSExecuteStatementResp resp = client
        .executeQueryStatement(new TSExecuteStatementReq(handle, statement, statementId));
    TSOperationHandle operationHandle = resp.getOperationHandle();
    System.out.println(resp.columns);

    TSFetchResultsResp tsFetchResultsResp = client
        .fetchResults(new TSFetchResultsReq(statement, 1000, operationHandle.operationId.queryId));
    List<RowRecord> rowRecordList = convertRowRecords(tsFetchResultsResp.queryDataSet, resp.dataTypeList);
    for (RowRecord record : rowRecordList) {
      System.out.println(record);
    }
  }

  public static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet,
      List<String> columnTypeList) {
    int rowCount = tsQueryDataSet.getRowCount();
    ByteBuffer byteBuffer = tsQueryDataSet.bufferForValues();

    // process time buffer
    List<RowRecord> rowRecordList = processTimeAndCreateRowRecords(byteBuffer, rowCount);

    for (String type : columnTypeList) {
      for (int i = 0; i < rowCount; i++) {
        Field field = null;
        boolean is_empty = BytesUtils.byteToBool(byteBuffer.get());
        if (is_empty) {
          field = new Field(null);
        } else {
          TSDataType dataType = TSDataType.valueOf(type);
          field = new Field(dataType);
          switch (dataType) {
            case BOOLEAN:
              boolean booleanValue = BytesUtils.byteToBool(byteBuffer.get());
              field.setBoolV(booleanValue);
              break;
            case INT32:
              int intValue = byteBuffer.getInt();
              field.setIntV(intValue);
              break;
            case INT64:
              long longValue = byteBuffer.getLong();
              field.setLongV(longValue);
              break;
            case FLOAT:
              float floatValue = byteBuffer.getFloat();
              field.setFloatV(floatValue);
              break;
            case DOUBLE:
              double doubleValue = byteBuffer.getDouble();
              field.setDoubleV(doubleValue);
              break;
            case TEXT:
              int binarySize = byteBuffer.getInt();
              byte[] binaryValue = new byte[binarySize];
              byteBuffer.get(binaryValue);
              field.setBinaryV(new Binary(binaryValue));
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
        rowRecordList.get(i).getFields().add(field);
      }
    }
    return rowRecordList;
  }

  private static List<RowRecord> processTimeAndCreateRowRecords(ByteBuffer byteBuffer,
      int rowCount) {
    List<RowRecord> rowRecordList = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      long timestamp = byteBuffer.getLong(); // byteBuffer has been flipped by the server side
      RowRecord rowRecord = new RowRecord(timestamp);
      rowRecordList.add(rowRecord);
    }
    return rowRecordList;
  }

  private static void testInsertion(Client client) throws TException, InterruptedException {
    System.out.println(client.setStorageGroup("root.beijing"));
    System.out.println(client.setStorageGroup("root.shanghai"));
    System.out.println(client.setStorageGroup("root.guangzhou"));
    System.out.println(client.setStorageGroup("root.shenzhen"));

    // wait until the storage group creations are committed
    Thread.sleep(3000);

    TSCreateTimeseriesReq req = new TSCreateTimeseriesReq();
    req.setDataType(TSDataType.DOUBLE.ordinal());
    req.setEncoding(TSEncoding.GORILLA.ordinal());
    req.setCompressor(CompressionType.SNAPPY.ordinal());
    req.setPath("root.beijing.d1.s1");
    System.out.println(client.createTimeseries(req));
    req.setPath("root.shanghai.d1.s1");
    System.out.println(client.createTimeseries(req));
    req.setPath("root.guangzhou.d1.s1");
    System.out.println(client.createTimeseries(req));
    req.setPath("root.shenzhen.d1.s1");
    System.out.println(client.createTimeseries(req));

    // wait until the timeseries creations are committed
    Thread.sleep(3000);

    TSInsertReq insertReq = new TSInsertReq();
    insertReq.setMeasurements(Collections.singletonList("s1"));
    for (int i = 0; i < 10; i ++) {
      insertReq.setTimestamp(i * 24 * 3600 * 1000L);
      insertReq.setValues(Collections.singletonList(Double.toString(i * 0.1)));
      insertReq.setDeviceId("root.beijing.d1");
      System.out.println(client.insertRow(insertReq));
      insertReq.setDeviceId("root.shanghai.d1");
      System.out.println(client.insertRow(insertReq));
      insertReq.setDeviceId("root.guangzhou.d1");
      System.out.println(client.insertRow(insertReq));
      insertReq.setDeviceId("root.shenzhen.d1");
      System.out.println(client.insertRow(insertReq));
    }
  }

}
