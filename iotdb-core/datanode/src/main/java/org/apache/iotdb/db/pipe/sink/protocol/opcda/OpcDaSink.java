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

package org.apache.iotdb.db.pipe.sink.protocol.opcda;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_ESCAPED_PATH_SEPARATOR_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_ESCAPED_PATH_SEPARATOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_ESCAPED_SEGMENT_ESCAPE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_ESCAPED_SEGMENT_ESCAPE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_NULL_TAG_SENTINEL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_NULL_TAG_SENTINEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_PROGID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_SEGMENT_ESCAPE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_SEGMENT_ESCAPE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_ESCAPED_PATH_SEPARATOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_ESCAPED_SEGMENT_ESCAPE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_NULL_TAG_SENTINEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_PROGID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_SEGMENT_ESCAPE_KEY;

/**
 * Send data in IoTDB based on Opc Da protocol, using JNA. All data are converted into tablets, and
 * then push the newest value to the <b>local COM</b> server in another process.
 *
 * <p>For table-model events, the OPC DA item id is generated as {@code <databaseName>.<deviceId
 * segments>.<field measurement>}. For example, if {@code tableModelDatabaseName=factory}, {@code
 * deviceId=status.d1} and the field is {@code s1}, the sink writes to OPC DA item id {@code
 * factory.status.d1.s1}.
 */
@TreeModel
@TableModel
public class OpcDaSink implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcDaSink.class);
  private static final Map<ServerHandleKey, Pair<AtomicInteger, OpcDaServerHandle>>
      CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP = new ConcurrentHashMap<>();
  private ServerHandleKey serverHandleKey;
  private OpcDaServerHandle handle;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    // TODO: upgrade this logic after "1 in 2" logic is supported
    validator.validate(
        args -> (boolean) args[0] || (boolean) args[1] || (boolean) args[2] || (boolean) args[3],
        String.format(
            "One of '%s', '%s', '%s' and '%s' must be specified",
            SINK_OPC_DA_CLSID_KEY,
            CONNECTOR_OPC_DA_CLSID_KEY,
            SINK_OPC_DA_PROGID_KEY,
            CONNECTOR_OPC_DA_PROGID_KEY),
        validator.getParameters().hasAttribute(SINK_OPC_DA_CLSID_KEY),
        validator.getParameters().hasAttribute(CONNECTOR_OPC_DA_CLSID_KEY),
        validator.getParameters().hasAttribute(SINK_OPC_DA_PROGID_KEY),
        validator.getParameters().hasAttribute(CONNECTOR_OPC_DA_PROGID_KEY));

    if (!System.getProperty("os.name").toLowerCase().startsWith("windows")) {
      throw new PipeParameterNotValidException("opc-da-sink must run on windows system.");
    }

    createTableModelItemIdEncodingConfig(validator.getParameters());
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    synchronized (CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP) {
      String clsID = parameters.getStringByKeys(CONNECTOR_OPC_DA_CLSID_KEY, SINK_OPC_DA_CLSID_KEY);
      if (Objects.isNull(clsID)) {
        clsID =
            OpcDaServerHandle.getClsIDFromProgID(
                parameters.getStringByKeys(CONNECTOR_OPC_DA_PROGID_KEY, SINK_OPC_DA_PROGID_KEY));
      }
      serverHandleKey =
          new ServerHandleKey(clsID, createTableModelItemIdEncodingConfig(parameters));
      handle =
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP
              .computeIfAbsent(
                  serverHandleKey,
                  key ->
                      new Pair<>(
                          new AtomicInteger(0),
                          new OpcDaServerHandle(key.clsID, key.tableModelItemIdEncodingConfig)))
              .getRight();
      CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.get(serverHandleKey).getLeft().incrementAndGet();
    }
  }

  @Override
  public void handshake() throws Exception {
    // Do nothing
  }

  @Override
  public void heartbeat() throws Exception {
    // Do nothing
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferByTablet(
        tabletInsertionEvent,
        (tablet, isTableModel, tableModelDatabaseName) ->
            handle.transfer(tablet, isTableModel, tableModelDatabaseName));
  }

  @Override
  public void transfer(final Event event) throws Exception {
    // Do nothing
  }

  @Override
  public void close() throws Exception {
    if (Objects.isNull(serverHandleKey)) {
      return;
    }

    synchronized (CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP) {
      final Pair<AtomicInteger, OpcDaServerHandle> pair =
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.get(serverHandleKey);
      if (pair == null) {
        return;
      }

      if (pair.getLeft().decrementAndGet() <= 0) {
        try {
          pair.getRight().close();
        } finally {
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.remove(serverHandleKey);
        }
      }
    }
  }

  private static OpcDaServerHandle.TableModelItemIdEncodingConfig
      createTableModelItemIdEncodingConfig(final PipeParameters parameters) {
    return new OpcDaServerHandle.TableModelItemIdEncodingConfig(
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_OPC_DA_NULL_TAG_SENTINEL_KEY, SINK_OPC_DA_NULL_TAG_SENTINEL_KEY),
            CONNECTOR_OPC_DA_NULL_TAG_SENTINEL_DEFAULT_VALUE),
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_OPC_DA_SEGMENT_ESCAPE_KEY, SINK_OPC_DA_SEGMENT_ESCAPE_KEY),
            CONNECTOR_OPC_DA_SEGMENT_ESCAPE_DEFAULT_VALUE),
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_OPC_DA_ESCAPED_SEGMENT_ESCAPE_KEY,
                SINK_OPC_DA_ESCAPED_SEGMENT_ESCAPE_KEY),
            CONNECTOR_OPC_DA_ESCAPED_SEGMENT_ESCAPE_DEFAULT_VALUE),
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_OPC_DA_ESCAPED_PATH_SEPARATOR_KEY,
                SINK_OPC_DA_ESCAPED_PATH_SEPARATOR_KEY),
            CONNECTOR_OPC_DA_ESCAPED_PATH_SEPARATOR_DEFAULT_VALUE));
  }

  private static void transferByTablet(
      final TabletInsertionEvent tabletInsertionEvent,
      final ThrowingTriConsumer<Tablet, Boolean, String, Exception> transferTablet)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "This Connector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletWrapper(
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent, transferTablet);
    } else {
      transferTabletWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent, transferTablet);
    }
  }

  private static void transferTabletWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final ThrowingTriConsumer<Tablet, Boolean, String, Exception> transferTablet)
      throws Exception {
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(OpcDaSink.class.getName())) {
      return;
    }
    try {
      final boolean isTableModel = pipeInsertNodeTabletInsertionEvent.isTableModelEvent();
      final String tableModelDatabaseName =
          isTableModel ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName() : null;
      for (final Tablet tablet : pipeInsertNodeTabletInsertionEvent.convertToTablets()) {
        transferTablet.accept(tablet, isTableModel, tableModelDatabaseName);
      }
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(OpcDaSink.class.getName(), false);
    }
  }

  private static void transferTabletWrapper(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent,
      final ThrowingTriConsumer<Tablet, Boolean, String, Exception> transferTablet)
      throws Exception {
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(OpcDaSink.class.getName())) {
      return;
    }
    try {
      final boolean isTableModel = pipeRawTabletInsertionEvent.isTableModelEvent();
      transferTablet.accept(
          pipeRawTabletInsertionEvent.convertToTablet(),
          isTableModel,
          isTableModel ? pipeRawTabletInsertionEvent.getTableModelDatabaseName() : null);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(OpcDaSink.class.getName(), false);
    }
  }

  @FunctionalInterface
  private interface ThrowingTriConsumer<T, U, V, E extends Exception> {
    void accept(final T t, final U u, final V v) throws E;
  }

  private static final class ServerHandleKey {
    private final String clsID;
    private final OpcDaServerHandle.TableModelItemIdEncodingConfig tableModelItemIdEncodingConfig;

    private ServerHandleKey(
        final String clsID,
        final OpcDaServerHandle.TableModelItemIdEncodingConfig tableModelItemIdEncodingConfig) {
      this.clsID = clsID;
      this.tableModelItemIdEncodingConfig = tableModelItemIdEncodingConfig;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ServerHandleKey)) {
        return false;
      }
      final ServerHandleKey that = (ServerHandleKey) obj;
      return Objects.equals(clsID, that.clsID)
          && Objects.equals(tableModelItemIdEncodingConfig, that.tableModelItemIdEncodingConfig);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clsID, tableModelItemIdEncodingConfig);
    }
  }
}
