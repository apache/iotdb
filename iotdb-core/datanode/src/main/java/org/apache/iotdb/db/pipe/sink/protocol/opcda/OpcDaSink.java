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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_DA_PROGID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_OPC_DA_PROGID_KEY;

/**
 * Send data in IoTDB based on Opc Da protocol, using JNA. All data are converted into tablets, and
 * then push the newest value to the <b>local COM</b> server in another process.
 */
public class OpcDaSink implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcDaSink.class);
  private static final Map<String, Pair<AtomicInteger, OpcDaServerHandle>>
      CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP = new ConcurrentHashMap<>();
  private String clsID;
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
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    synchronized (CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP) {
      clsID = parameters.getStringByKeys(CONNECTOR_OPC_DA_CLSID_KEY, SINK_OPC_DA_CLSID_KEY);
      if (Objects.isNull(clsID)) {
        clsID =
            OpcDaServerHandle.getClsIDFromProgID(
                parameters.getStringByKeys(CONNECTOR_OPC_DA_PROGID_KEY, SINK_OPC_DA_PROGID_KEY));
      }
      handle =
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP
              .computeIfAbsent(
                  clsID, key -> new Pair<>(new AtomicInteger(0), new OpcDaServerHandle(clsID)))
              .getRight();
      CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.get(clsID).getLeft().incrementAndGet();
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
    transferByTablet(tabletInsertionEvent, LOGGER, tablet -> handle.transfer(tablet));
  }

  @Override
  public void transfer(final Event event) throws Exception {
    // Do nothing
  }

  private static void transferByTablet(
      final TabletInsertionEvent tabletInsertionEvent,
      final Logger logger,
      final ThrowingConsumer<Tablet, Exception> transferTablet)
      throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      logger.warn(
          DataNodePipeMessages
              .LOG_THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETINSERTIONEVENT_IGNORE_ARG_9D8780C0,
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
      final ThrowingConsumer<Tablet, Exception> transferTablet)
      throws Exception {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(OpcDaSink.class.getName())) {
      return;
    }
    try {
      for (final Tablet tablet : pipeInsertNodeTabletInsertionEvent.convertToTablets()) {
        transferTablet.accept(tablet);
      }
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(OpcDaSink.class.getName(), false);
    }
  }

  private static void transferTabletWrapper(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent,
      final ThrowingConsumer<Tablet, Exception> transferTablet)
      throws Exception {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(OpcDaSink.class.getName())) {
      return;
    }
    try {
      transferTablet.accept(pipeRawTabletInsertionEvent.convertToTablet());
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(OpcDaSink.class.getName(), false);
    }
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T, E extends Exception> {
    void accept(final T t) throws E;
  }

  @Override
  public void close() throws Exception {
    if (Objects.isNull(clsID)) {
      return;
    }

    synchronized (CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP) {
      final Pair<AtomicInteger, OpcDaServerHandle> pair =
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.get(clsID);
      if (pair == null) {
        return;
      }

      if (pair.getLeft().decrementAndGet() <= 0) {
        try {
          pair.getRight().close();
        } finally {
          CLS_ID_TO_REFERENCE_COUNT_AND_HANDLE_MAP.remove(clsID);
        }
      }
    }
  }
}
