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

import org.apache.iotdb.flink.sql.client.IoTDBWebsocketClient;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.flink.sql.wrapper.TabletWrapper;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.enums.ReadyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class IoTDBCDCSourceFunction<RowData> extends RichSourceFunction<RowData> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBCDCSourceFunction.class);
  private final List<IoTDBWebsocketClient> socketClients = new ArrayList<>();
  private final List<String> nodeUrls;
  private final String device;
  private final List<String> measurements;
  private final BlockingQueue<TabletWrapper> tabletWrappers;
  private transient ExecutorService consumeExecutor;

  public IoTDBCDCSourceFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    List<Tuple2<String, DataType>> tableSchema = schemaWrapper.getSchema();
    nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).split(","));
    device = options.get(Options.DEVICE);
    measurements =
        tableSchema.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());

    tabletWrappers = new ArrayBlockingQueue<>(nodeUrls.size());
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    consumeExecutor = Executors.newFixedThreadPool(1);
    for (String nodeUrl : nodeUrls) {
      URI uri = new URI(String.format("ws://%s", nodeUrl));
      socketClients.add(initAndGet(uri));
    }
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws InterruptedException, URISyntaxException {
    consumeExecutor.submit(new ConsumeRunnable(ctx));
    consumeExecutor.shutdown();
    while (true) {
      for (int i = 0; i < socketClients.size(); i++) {
        if (socketClients.get(i).getReadyState().equals(ReadyState.CLOSED)) {
          String nodeUrl = nodeUrls.get(i);
          try {
            URI uri = new URI(String.format("ws://%s", nodeUrl));
            socketClients.set(i, initAndGet(uri));
          } catch (URISyntaxException e) {
            String log = String.format("Unable to create an URI by nodeUrl: %s", nodeUrl);
            LOGGER.error(log);
            throw e;
          }
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
      String host = tabletWrapper.getWebsocketClient().getRemoteSocketAddress().getHostName();
      int port = tabletWrapper.getWebsocketClient().getRemoteSocketAddress().getPort();
      String log =
          String.format(
              "The tablet from %s:%d can't be put into queue, because: %s",
              host, port, e.getMessage());
      LOGGER.warn(log);
      Thread.currentThread().interrupt();
    }
  }

  private IoTDBWebsocketClient initAndGet(URI uri) throws InterruptedException {
    IoTDBWebsocketClient client = new IoTDBWebsocketClient(uri, this);
    client.connect();
    while (!client.getReadyState().equals(ReadyState.OPEN)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }
    client.send("START");
    return client;
  }

  public void collectTablet(Tablet tablet, SourceContext<RowData> ctx) {
    if (!device.equals(tablet.deviceId)) {
      return;
    }
    List<MeasurementSchema> schemas = tablet.getSchemas();
    int rowSize = tablet.rowSize;
    HashMap<String, Pair<BitMap, List<Object>>> values = new HashMap<>();
    for (MeasurementSchema schema : schemas) {
      String measurement = schema.getMeasurementId();
      values.put(
          measurement,
          new Pair<>(
              tablet.bitMaps[schemas.indexOf(schema)],
              Utils.object2List(tablet.values[schemas.indexOf(schema)], schema.getType())));
    }
    for (int i = 0; i < rowSize; i++) {
      ArrayList<Object> row = new ArrayList<>();
      row.add(tablet.timestamps[i]);
      for (String measurement : measurements) {
        if (values.get(measurement).getLeft() == null
            || !values.get(measurement).getLeft().isMarked(i)) {
          row.add(values.get(measurement).getRight().get(i));
        } else {
          row.add(null);
        }
      }
      RowData rowData = (RowData) GenericRowData.of(row.toArray());
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
              .getWebsocketClient()
              .send(String.format("ACK:%d", tabletWrapper.getCommitId()));
        } catch (InterruptedException e) {
          LOGGER.warn("The tablet can't be taken from queue!");
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
