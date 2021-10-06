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

package org.apache.iotdb.cluster;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client.Factory;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("java:S106")
public class ClientMain {

  private static final Logger logger = LoggerFactory.getLogger(ClientMain.class);

  private static final String PARAM_INSERTION = "i";
  private static final String PARAM_QUERY = "q";
  private static final String PARAM_DELETE_STORAGE_GROUP = "dsg";
  private static final String PARAM_DELETE_SERIES = "ds";
  private static final String PARAM_QUERY_PORTS = "qp";
  private static final String PARAM_INSERT_PORT = "ip";
  private static final String PARAM_BATCH = "b";
  private static Options options = new Options();

  private static String ip = "127.0.0.1";
  private static int port = 6667;

  static {
    options.addOption(new Option(PARAM_INSERTION, "Perform insertion"));
    options.addOption(new Option(PARAM_QUERY, "Perform query"));
    options.addOption(new Option(PARAM_DELETE_SERIES, "Perform deleting timeseries"));
    options.addOption(new Option(PARAM_DELETE_STORAGE_GROUP, "Perform deleting storage group"));
    options.addOption(
        new Option(PARAM_QUERY_PORTS, true, "Ports to query (ip is currently " + "localhost)"));
    options.addOption(new Option(PARAM_INSERT_PORT, true, "Port to perform insertion"));
    options.addOption(new Option(PARAM_BATCH, "Test batch statement"));
  }

  private static Map<String, TSStatus> failedQueries;

  private static final String[] STORAGE_GROUPS =
      new String[] {
        "root.beijing", "root.shanghai", "root.guangzhou", "root.shenzhen",
      };

  private static final String[] DEVICES =
      new String[] {
        "root.beijing.d1", "root.shanghai.d1", "root.guangzhou.d1", "root.shenzhen.d1",
      };

  private static final String[] MEASUREMENTS = new String[] {"s1"};

  private static final TSDataType[] DATA_TYPES = new TSDataType[] {TSDataType.DOUBLE};

  private static List<IMeasurementSchema> schemas;

  private static final String[] DATA_QUERIES =
      new String[] {
        // raw data multi series
        "SELECT * FROM root",
        "SELECT * FROM root WHERE time <= 691200000",
        "SELECT * FROM root WHERE time >= 391200000 and time <= 691200000",
        "SELECT * FROM root.*.* WHERE s1 <= 0.7",
        // raw data single series
        "SELECT s1 FROM root.beijing.d1",
        "SELECT s1 FROM root.shanghai.d1",
        "SELECT s1 FROM root.guangzhou.d1",
        "SELECT s1 FROM root.shenzhen.d1",
        // aggregation
        "SELECT count(s1) FROM root.*.*",
        "SELECT avg(s1) FROM root.*.*",
        "SELECT sum(s1) FROM root.*.*",
        "SELECT max_value(s1) FROM root.*.*",
        "SELECT count(s1) FROM root.*.* where time <= 691200000",
        "SELECT count(s1) FROM root.*.* where s1 <= 0.7",
        // group by device
        "SELECT * FROM root GROUP BY DEVICE",
        // fill
        "SELECT s1 FROM root.beijing.d1 WHERE time = 86400000 FILL (DOUBLE[PREVIOUS,1d])",
        "SELECT s1 FROM root.shanghai.d1 WHERE time = 86400000 FILL (DOUBLE[LINEAR,1d,1d])",
        "SELECT s1 FROM root.guangzhou.d1 WHERE time = 126400000 FILL (DOUBLE[PREVIOUS,1d])",
        "SELECT s1 FROM root.shenzhen.d1 WHERE time = 126400000 FILL (DOUBLE[LINEAR,1d,1d])",
        // group by
        "SELECT COUNT(*) FROM root.*.* GROUP BY ([0, 864000000), 3d, 3d)",
        "SELECT AVG(*) FROM root.*.* WHERE s1 <= 0.7 GROUP BY ([0, 864000000), 3d, 3d)",
        // last
        "SELECT LAST s1 FROM root.*.*",
      };

  private static final String[] META_QUERY =
      new String[] {
        "SHOW STORAGE GROUP",
        "SHOW TIMESERIES root",
        "COUNT TIMESERIES root",
        "COUNT TIMESERIES root GROUP BY LEVEL=2",
        "SHOW DEVICES",
        "SHOW TIMESERIES root limit 1 offset 1",
      };

  public static void main(String[] args)
      throws TException, StatementExecutionException, IoTDBConnectionException, ParseException,
          SQLException, ClassNotFoundException {
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
    boolean noOption = args.length == 0;

    failedQueries = new HashMap<>();
    prepareSchema();

    if (commandLine.hasOption(PARAM_INSERT_PORT)) {
      port = Integer.parseInt(commandLine.getOptionValue(PARAM_INSERT_PORT));
    }

    doInsertion(noOption, commandLine);

    doQuery(noOption, commandLine);

    doDeleteSeries(noOption, commandLine);

    doDeleteSG(noOption, commandLine);

    doBatchStmt(noOption, commandLine);
  }

  private static void doInsertion(boolean noOption, CommandLine commandLine) throws TException {
    if (noOption || commandLine.hasOption(PARAM_INSERTION)) {
      System.out.println("Test insertion");
      Client client = getClient(ip, port);
      long sessionId = connectClient(client);
      testInsertion(client, sessionId);
      client.closeSession(new TSCloseSessionReq(sessionId));
    }
  }

  private static void doQuery(boolean noOption, CommandLine commandLine)
      throws StatementExecutionException, TException, IoTDBConnectionException {
    if (noOption || commandLine.hasOption(PARAM_QUERY)) {
      int[] queryPorts = null;
      if (commandLine.hasOption(PARAM_QUERY_PORTS)) {
        queryPorts = parseIntArray(commandLine.getOptionValue(PARAM_QUERY_PORTS));
      }
      if (queryPorts == null) {
        queryPorts = new int[] {port, port + 1, port + 2};
      }
      for (int queryPort : queryPorts) {
        System.out.println("Test port: " + queryPort);

        Client client = getClient(ip, queryPort);
        long sessionId = connectClient(client);
        System.out.println("Test data queries");
        testQuery(client, sessionId, DATA_QUERIES);

        System.out.println("Test metadata queries");
        testQuery(client, sessionId, META_QUERY);

        logger.info("Failed queries: {}", failedQueries);
        client.closeSession(new TSCloseSessionReq(sessionId));
      }
    }
  }

  private static void doDeleteSeries(boolean noOption, CommandLine commandLine) throws TException {
    if (noOption || commandLine.hasOption(PARAM_DELETE_SERIES)) {
      System.out.println("Test delete timeseries");
      Client client = getClient(ip, port);
      long sessionId = connectClient(client);
      testDeleteTimeseries(client, sessionId);
      client.closeSession(new TSCloseSessionReq(sessionId));
    }
  }

  private static void doDeleteSG(boolean noOption, CommandLine commandLine)
      throws StatementExecutionException, TException, IoTDBConnectionException {
    if (noOption || commandLine.hasOption(PARAM_DELETE_STORAGE_GROUP)) {
      System.out.println("Test delete storage group");
      Client client = getClient(ip, port);
      long sessionId = connectClient(client);
      testDeleteStorageGroup(client, sessionId);
      client.closeSession(new TSCloseSessionReq(sessionId));
    }
  }

  private static void doBatchStmt(boolean noOption, CommandLine commandLine)
      throws SQLException, ClassNotFoundException {
    if (noOption || commandLine.hasOption(PARAM_BATCH)) {
      System.out.println("Test batch create sgs");
      testBatch(ip, port);
    }
  }

  private static int[] parseIntArray(String str) {
    if (str == null) {
      return new int[0];
    }
    String[] split = str.split(",");
    int[] ret = new int[split.length];
    for (int i = 0; i < split.length; i++) {
      ret[i] = Integer.parseInt(split[i]);
    }
    return ret;
  }

  private static long connectClient(Client client) throws TException {
    TSOpenSessionReq openReq =
        new TSOpenSessionReq(
            TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3, ZoneId.systemDefault().getId());
    openReq.setUsername("root");
    openReq.setPassword("root");
    TSOpenSessionResp openResp = client.openSession(openReq);
    return openResp.getSessionId();
  }

  @SuppressWarnings({"java:S2095"}) // the transport is used later
  private static Client getClient(String ip, int port) throws TTransportException {
    TSIService.Client.Factory factory = new Factory();
    TTransport transport = RpcTransportFactory.INSTANCE.getTransport(new TSocket(ip, port));
    transport.open();
    TProtocol protocol =
        IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()
            ? new TCompactProtocol(transport)
            : new TBinaryProtocol(transport);
    return factory.getClient(protocol);
  }

  private static void prepareSchema() {
    schemas = new ArrayList<>();
    for (String device : DEVICES) {
      for (int i = 0; i < MEASUREMENTS.length; i++) {
        String measurement = MEASUREMENTS[i];
        schemas.add(
            new UnaryMeasurementSchema(
                device + IoTDBConstant.PATH_SEPARATOR + measurement, DATA_TYPES[i]));
      }
    }
  }

  private static void testQuery(Client client, long sessionId, String[] queries)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    long statementId = client.requestStatementId(sessionId);
    for (String dataQuery : queries) {
      executeQuery(client, sessionId, dataQuery, statementId);
    }

    TSCloseOperationReq tsCloseOperationReq = new TSCloseOperationReq(sessionId);
    tsCloseOperationReq.setStatementId(statementId);
    client.closeOperation(tsCloseOperationReq);
  }

  private static void executeQuery(Client client, long sessionId, String query, long statementId)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    if (logger.isInfoEnabled()) {
      logger.info("{ {} }", query);
    }
    TSExecuteStatementResp resp =
        client.executeQueryStatement(
            new TSExecuteStatementReq(sessionId, query, statementId).setFetchSize(1000));
    if (resp.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      failedQueries.put(query, resp.status);
      return;
    }

    long queryId = resp.getQueryId();
    if (logger.isInfoEnabled()) {
      logger.info(resp.columns.toString());
    }

    SessionDataSet dataSet =
        new SessionDataSet(
            query,
            resp.getColumns(),
            resp.getDataTypeList(),
            resp.columnNameIndexMap,
            queryId,
            statementId,
            client,
            sessionId,
            resp.queryDataSet,
            false);

    while (dataSet.hasNext()) {
      if (logger.isInfoEnabled()) {
        logger.info(dataSet.next().toString());
      }
    }
    System.out.println();

    TSCloseOperationReq tsCloseOperationReq = new TSCloseOperationReq(sessionId);
    tsCloseOperationReq.setQueryId(queryId);
    client.closeOperation(tsCloseOperationReq);
  }

  private static void testDeleteStorageGroup(Client client, long sessionId)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    if (logger.isInfoEnabled()) {
      logger.info(client.deleteStorageGroups(sessionId, Arrays.asList(STORAGE_GROUPS)).toString());
    }

    testQuery(client, sessionId, new String[] {"SELECT * FROM root"});
  }

  private static void registerTimeseries(long sessionId, Client client) throws TException {
    TSCreateTimeseriesReq req = new TSCreateTimeseriesReq();
    req.setSessionId(sessionId);
    for (IMeasurementSchema schema : schemas) {
      req.setDataType(schema.getType().ordinal());
      req.setEncoding(schema.getEncodingType().ordinal());
      req.setCompressor(schema.getCompressor().ordinal());
      req.setPath(schema.getMeasurementId());
      if (logger.isInfoEnabled()) {
        logger.info(client.createTimeseries(req).toString());
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  private static void testInsertion(Client client, long sessionId) throws TException {
    for (String storageGroup : STORAGE_GROUPS) {
      if (logger.isInfoEnabled()) {
        logger.info(client.setStorageGroup(sessionId, storageGroup).toString());
      }
    }

    registerTimeseries(sessionId, client);

    TSInsertStringRecordReq insertReq = new TSInsertStringRecordReq();
    insertReq.setMeasurements(Arrays.asList(MEASUREMENTS));
    insertReq.setSessionId(sessionId);

    for (int i = 0; i < 10; i++) {
      List<String> values = new ArrayList<>(MEASUREMENTS.length);
      insertReq.setTimestamp(i * 24 * 3600 * 1000L);
      for (int i1 = 0; i1 < MEASUREMENTS.length; i1++) {
        switch (DATA_TYPES[i1]) {
          case DOUBLE:
            values.add(Double.toString(i * 0.1));
            break;
          case BOOLEAN:
            values.add(Boolean.toString(i % 2 == 0));
            break;
          case INT64:
            values.add(Long.toString(i));
            break;
          case INT32:
            values.add(Integer.toString(i));
            break;
          case FLOAT:
            values.add(Float.toString(i * 0.1f));
            break;
          case TEXT:
            values.add("S" + i);
            break;
        }
      }

      insertReq.setValues(values);

      for (String device : DEVICES) {
        insertReq.setPrefixPath(device);
        if (logger.isInfoEnabled()) {
          logger.info(insertReq.toString());
          logger.info(client.insertStringRecord(insertReq).toString());
        }
      }
    }
  }

  private static void testDeleteTimeseries(Client client, long sessionId) throws TException {
    List<String> paths = new ArrayList<>();
    for (String measurement : MEASUREMENTS) {
      for (String device : DEVICES) {
        paths.add(device + "." + measurement);
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info(client.deleteTimeseries(sessionId, paths).toString());
    }
  }

  private static void testBatch(String ip, int port) throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + String.format("%s:%d/", ip, port), "root", "root");
        Statement statement = connection.createStatement()) {

      statement.addBatch("SET STORAGE GROUP TO root.batch1");
      statement.addBatch("SET STORAGE GROUP TO root.batch2");
      statement.addBatch("SET STORAGE GROUP TO root.batch3");
      statement.addBatch("SET STORAGE GROUP TO root.batch4");

      statement.executeBatch();
      statement.clearBatch();

      try (ResultSet set = statement.executeQuery("SHOW STORAGE GROUP")) {
        int colNum = set.getMetaData().getColumnCount();
        while (set.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 0; i < colNum; i++) {
            stringBuilder.append(set.getString(i + 1)).append(",");
          }
          System.out.println(stringBuilder);
        }
      }
    }
  }
}
