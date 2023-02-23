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

import org.apache.iotdb.pipe.api.customizer.config.ConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeValidator;
import org.apache.iotdb.pipe.api.event.DeletionEvent;
import org.apache.iotdb.pipe.api.event.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.TsFileInsertionEvent;

/**
 * PipeConnector
 *
 * <p>PipeConnector as the network layer, supports access to multiple transport protocols such as
 * ThriftRPC, HTTP, TCP, etc. for transferring data.
 *
 * <p>The lifecycle of a PipeConnector is as follows:
 *
 * <ul>
 *   <li>Before the sync task starts, the KV pair of `WITH CONNECTOR` clause in SQL is parsed and
 *       the validate method {@link PipeConnector#validate(PipeValidator)}is called to validate the
 *       parameters.
 *   <li>When the sync task starts, load and initialize the PipeConnector instance, and then call
 *       the beforeStart method {@link PipeConnector#beforeStart(PipeParameters,
 *       ConnectorRuntimeConfiguration)}
 *   <li>while the sync task is in progress
 *       <ul>
 *         <li>PipeConnector calls the handshake method to establish connections between servers
 *         <li>PipeConnector calls the transfer method to transfer data
 *         <li>PipeConnector provides heartbeat detection method
 *       </ul>
 *   <li>When the sync task is stopped, the PipeConnector stops transferring data.
 *   <li>When the sync task is cancelled, the PipeConnector calls the autoClose method.
 * </ul>
 */
public interface PipeConnector extends AutoCloseable {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeConnector. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in ConnectorRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the PipeConnector is instantiated and before the beginning of
   * the server connection.
   *
   * @param params used to parse the input parameters entered by the user
   * @param config used to set the required properties of the running PipeConnector
   * @throws Exception the user can throw errors if necessary
   */
  void beforeStart(PipeParameters params, ConnectorRuntimeConfiguration config) throws Exception;

  /**
   * PipeConnector use PipeParameters which set in the method {@link
   * PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)} to establish the
   * handshake connection between servers.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void handshake() throws Exception;

  /**
   * This method is called to check whether the PipeConnector is connected to the servers.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void heartbeat() throws Exception;

  /**
   * This method is called to transfer the TabletInsertionEvent.
   *
   * @param te the insertion event of Tablet
   * @throws Exception the user can throw errors if necessary
   */
  default void transfer(TabletInsertionEvent te) throws Exception {}

  /**
   * This method is called to transfer the TsFileInsertionEvent.
   *
   * @param te the insertion event of TsFile
   * @throws Exception the user can throw errors if necessary
   */
  default void transfer(TsFileInsertionEvent te) throws Exception {}

  /**
   * This method is called to transfer the DeletionEvent.
   *
   * @param de the insertion event of Deletion
   * @throws Exception the user can throw errors if necessary
   */
  default void transfer(DeletionEvent de) throws Exception {}
}
