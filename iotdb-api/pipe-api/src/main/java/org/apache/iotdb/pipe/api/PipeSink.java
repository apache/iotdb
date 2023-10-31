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

package org.apache.iotdb.pipe.api;

import org.apache.iotdb.pipe.api.customizer.configuration.PipeSinkRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

/**
 * PipeSink
 *
 * <p>PipeSink is responsible for sending events to sinks.
 *
 * <p>Various network protocols can be supported by implementing different PipeSink classes.
 *
 * <p>The lifecycle of a PipeSink is as follows:
 *
 * <ul>
 *   <li>When a collaboration task is created, the KV pairs of `WITH CONNECTOR` clause in SQL are
 *       parsed and the validation method {@link PipeSink#validate(PipeParameterValidator)} will be
 *       called to validate the parameters.
 *   <li>Before the collaboration task starts, the method {@link PipeSink#customize(PipeParameters,
 *       PipeSinkRuntimeConfiguration)} will be called to config the runtime behavior of the
 *       PipeSink and the method {@link PipeSink#handshake()} will be called to create a connection
 *       with sink.
 *   <li>While the collaboration task is in progress:
 *       <ul>
 *         <li>PipeExtractor captures the events and wraps them into three types of Event instances.
 *         <li>PipeProcessor processes the event and then passes them to the PipeSink.
 *         <li>PipeSink serializes the events into binaries and send them to sinks. The following 3
 *             methods will be called: {@link PipeSink#transfer(TabletInsertionEvent)}, {@link
 *             PipeSink#transfer(TsFileInsertionEvent)} and {@link PipeSink#transfer(Event)}.
 *       </ul>
 *   <li>When the collaboration task is cancelled (the `DROP PIPE` command is executed), the {@link
 *       PipeSink#close() } method will be called.
 * </ul>
 *
 * <p>In addition, the method {@link PipeSink#heartbeat()} will be called periodically to check
 * whether the connection with sink is still alive. The method {@link PipeSink#handshake()} will be
 * called to create a new connection with the sink when the method {@link PipeSink#heartbeat()}
 * throws exceptions.
 */
public interface PipeSink extends PipeConnector {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeSink#customize(PipeParameters, PipeSinkRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeParameterValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeSink. In this method, the user can do the following
   * things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in PipeSinkRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the method {@link PipeSink#validate(PipeParameterValidator)} is
   * called and before the method {@link PipeSink#handshake()} is called.
   *
   * @param parameters used to parse the input parameters entered by the user
   * @param configuration used to set the required properties of the running PipeSink
   * @throws Exception the user can throw errors if necessary
   */
  void customize(PipeParameters parameters, PipeSinkRuntimeConfiguration configuration)
      throws Exception;

  /**
   * This method is used to create a connection with sink. This method will be called after the
   * method {@link PipeSink#customize(PipeParameters, PipeSinkRuntimeConfiguration)} is called or
   * will be called when the method {@link PipeSink#heartbeat()} throws exceptions.
   *
   * @throws Exception if the connection is failed to be created
   */
  void handshake() throws Exception;

  /**
   * This method will be called periodically to check whether the connection with sink is still
   * alive.
   *
   * @throws Exception if the connection dies
   */
  void heartbeat() throws Exception;

  /**
   * This method is used to transfer the TabletInsertionEvent.
   *
   * @param tabletInsertionEvent TabletInsertionEvent to be transferred
   * @throws PipeConnectionException if the connection is broken
   * @throws Exception the user can throw errors if necessary
   */
  void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception;

  /**
   * This method is used to transfer the TsFileInsertionEvent.
   *
   * @param tsFileInsertionEvent TsFileInsertionEvent to be transferred
   * @throws PipeConnectionException if the connection is broken
   * @throws Exception the user can throw errors if necessary
   */
  default void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    try {
      for (final TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        transfer(tabletInsertionEvent);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  /**
   * This method is used to transfer the generic events, including HeartbeatEvent.
   *
   * @param event Event to be transferred
   * @throws PipeConnectionException if the connection is broken
   * @throws Exception the user can throw errors if necessary
   */
  void transfer(Event event) throws Exception;
}
