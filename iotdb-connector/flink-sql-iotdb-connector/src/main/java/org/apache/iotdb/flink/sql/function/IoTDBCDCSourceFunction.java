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
package org.apache.iotdb.flink.sql.function;

import org.apache.iotdb.flink.sql.client.IoTDBWebSocketClient;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.IllegalOptionException;
import org.apache.iotdb.flink.sql.exception.IllegalSchemaException;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.flink.sql.wrapper.TabletWrapper;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.enums.ReadyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class IoTDBCDCSourceFunction extends RichSourceFunction<RowData> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBCDCSourceFunction.class);
  private final List<IoTDBWebSocketClient> socketClients = new ArrayList<>();
  private final int cdcPort;
  private final List<String> nodeUrls;
  private final String taskName;
  private final String pattern;
  private final String user;
  private final String password;
  private final List<String> timeseriesList;
  private final BlockingQueue<TabletWrapper> tabletWrappers;
  private final List<Tuple2<String, DataType>> tableSchema;
  private final String pipeName;
  private final Options.CDCMode mode;
  private transient ExecutorService consumeExecutor;

  public IoTDBCDCSourceFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    tableSchema = schemaWrapper.getSchema();
    pattern = options.get(Options.PATTERN);
    cdcPort = options.get(Options.CDC_PORT);
    nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).split(","));
    taskName = options.get(Options.CDC_TASK_NAME);
    pipeName = String.format("flink_cdc_%s", taskName);
    user = options.get(Options.USER);
    password = options.get(Options.PASSWORD);
    timeseriesList =
        tableSchema.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
    mode = options.get(Options.CDC_MODE);

    tabletWrappers = new ArrayBlockingQueue<>(nodeUrls.size() * 10);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    Session session =
        new Session.Builder().username(user).password(password).nodeUrls(nodeUrls).build();
    session.open(false);
    try (SessionDataSet dataSet =
        session.executeQueryStatement(String.format("show pipe %s", pipeName))) {
      if (!dataSet.hasNext()) {
        String createPipeCommand;
        if (Options.CDCMode.REALTIME.equals(mode)) {
          createPipeCommand =
              String.format(
                  "CREATE PIPE %s\n"
                      + "WITH EXTRACTOR (\n"
                      + "'extractor' = 'iotdb-extractor',\n"
                      + "'extractor.history.enable' = 'false',\n"
                      + "'extractor.pattern' = '%s',\n"
                      + ") WITH CONNECTOR (\n"
                      + "'connector' = 'websocket-connector',\n"
                      + "'connector.websocket.port' = '%d',\n"
                      // avoid to reuse the pipe's connector
                      + "'connector.websocket.id' = '%d'"
                      + ")",
                  pipeName, pattern, cdcPort, System.currentTimeMillis());
        } else {
          createPipeCommand =
              String.format(
                  "CREATE PIPE %s\n"
                      + "WITH EXTRACTOR (\n"
                      + "'extractor' = 'iotdb-extractor',\n"
                      + "'extractor.pattern' = '%s',\n"
                      + ") WITH CONNECTOR (\n"
                      + "'connector' = 'websocket-connector',\n"
                      + "'connector.websocket.port' = '%d',\n"
                      // avoid to reuse the pipe's connector
                      + "'connector.websocket.id' = '%d'"
                      + ")",
                  pipeName, pattern, cdcPort, System.currentTimeMillis());
        }

        session.executeNonQueryStatement(createPipeCommand);
        session.executeNonQueryStatement(String.format("start pipe %s", pipeName));
      } else {
        RowRecord pipe = dataSet.next();
        String pipePattern = pipe.getFields().get(3).getStringValue().split(",")[0].split("=")[1];
        if (!pipePattern.equals(this.pattern)) {
          throw new IllegalOptionException(
              String.format(
                  "The CDC task `%s` has been created by pattern `%s`.",
                  this.taskName, this.pattern));
        }
        String status = pipe.getFields().get(2).getStringValue();
        if ("STOPPED".equals(status)) {
          session.executeNonQueryStatement(String.format("start pipe %s", pipeName));
        }
      }
    }
    session.close();

    consumeExecutor = Executors.newFixedThreadPool(1);
    for (String nodeUrl : nodeUrls) {
      URI uri = new URI(String.format("ws://%s:%s", nodeUrl.split(":")[0], cdcPort));
      socketClients.add(initAndGet(uri));
    }
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws InterruptedException {
    consumeExecutor.execute(new ConsumeRunnable(ctx));
    consumeExecutor.shutdown();
    while (true) {
      if (consumeExecutor.isTerminated()) {
        System.exit(1);
      }
      for (IoTDBWebSocketClient socketClient : socketClients) {
        if (socketClient.getReadyState().equals(ReadyState.CLOSED)) {
          while (!Utils.isURIAvailable(socketClient.getURI())) {
            String log =
                String.format(
                    "The URI %s:%d is not available now, sleep 5 seconds.",
                    socketClient.getURI().getHost(), socketClient.getURI().getPort());
            LOGGER.warn(log);
            Thread.sleep(5000);
          }
          socketClient.reconnect();
          while (!socketClient.getReadyState().equals(ReadyState.OPEN)) {
            Thread.sleep(1000);
          }
          socketClient.send(String.format("BIND:%s", pipeName));
        } else {
          Thread.sleep(1000);
        }
      }
    }
  }

  @Override
  public void cancel() {
    socketClients.forEach(WebSocketClient::close);
  }

  public void addTabletWrapper(TabletWrapper tabletWrapper) {
    try {
      this.tabletWrappers.put(tabletWrapper);
    } catch (InterruptedException e) {
      String host = tabletWrapper.getWebSocketClient().getRemoteSocketAddress().getHostName();
      int port = tabletWrapper.getWebSocketClient().getRemoteSocketAddress().getPort();
      String log =
          String.format(
              "The tablet from %s:%d can't be put into queue, because: %s",
              host, port, e.getMessage());
      LOGGER.warn(log);
      Thread.currentThread().interrupt();
    }
  }

  private IoTDBWebSocketClient initAndGet(URI uri) throws InterruptedException {
    while (!Utils.isURIAvailable(uri)) {
      String log =
          String.format(
              "The URI %s:%d is not available now, sleep 5 seconds.", uri.getHost(), uri.getPort());
      LOGGER.warn(log);
      Thread.sleep(5000);
    }
    IoTDBWebSocketClient client = new IoTDBWebSocketClient(uri, this);
    client.connect();
    while (!client.getReadyState().equals(ReadyState.OPEN)) {
      Thread.sleep(1000);
    }
    client.send(String.format("BIND:%s", pipeName));
    while (IoTDBWebSocketClient.Status.WAITING == client.getStatus()) {
      Thread.sleep(1000);
    }
    if (IoTDBWebSocketClient.Status.ERROR == client.getStatus()) {
      throw new IllegalOptionException(
          "An exception occurred during binding. The CDC task is running. Please stop it first.");
    }
    return client;
  }

  public void collectTablet(Tablet tablet, SourceContext<RowData> ctx) {
    List<MeasurementSchema> schemas = tablet.getSchemas();
    int rowSize = tablet.rowSize;
    HashMap<String, Pair<BitMap, List<Object>>> values = new HashMap<>();
    for (MeasurementSchema schema : schemas) {
      String timeseries = String.format("%s.%s", tablet.deviceId, schema.getMeasurementId());
      TSDataType iotdbType = schema.getType();
      int index = timeseriesList.indexOf(timeseries);
      if (index == -1) {
        return;
      }
      DataType flinkType = tableSchema.get(index).f1;
      if (!Utils.isTypeEqual(iotdbType, flinkType)) {
        throw new IllegalSchemaException(
            String.format(
                "The data type of column `%s` is different in IoTDB and Flink", timeseries));
      }
      values.put(
          timeseries,
          new Pair<>(
              tablet.bitMaps[schemas.indexOf(schema)],
              Utils.object2List(tablet.values[schemas.indexOf(schema)], iotdbType)));
    }
    for (int i = 0; i < rowSize; i++) {
      ArrayList<Object> row = new ArrayList<>();
      row.add(tablet.timestamps[i]);
      for (String timeseries : timeseriesList) {
        if (values.containsKey(timeseries)
            && (values.get(timeseries).getLeft() == null
                || !values.get(timeseries).getLeft().isMarked(i))) {
          row.add(values.get(timeseries).getRight().get(i));
        } else {
          row.add(null);
        }
      }
      RowData rowData = GenericRowData.of(row.toArray());
      ctx.collect(rowData);
    }
  }

  private class ConsumeRunnable implements Runnable {
    SourceContext<RowData> context;

    public ConsumeRunnable(SourceContext<RowData> context) {
      this.context = context;
    }

    @Override
    public void run() {
      while (true) {
        try {
          TabletWrapper tabletWrapper = tabletWrappers.take();
          collectTablet(tabletWrapper.getTablet(), context);
          tabletWrapper
              .getWebSocketClient()
              .send(String.format("ACK:%d", tabletWrapper.getCommitId()));
        } catch (InterruptedException e) {
          LOGGER.warn("The tablet can't be taken from queue!");
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
