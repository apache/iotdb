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

package org.apache.iotdb.db.pipe.connector.protocol.opcda;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import com.sun.jna.platform.win32.Guid;

/**
 * Send data in IoTDB based on Opc Da protocol, using JNA. All data are converted into tablets, and
 * then push the newest value to the <b>local COM</b> server in another process.
 */
@TreeModel
public class OPCDAConnector implements PipeConnector {

  private Guid.CLSID CLSID_OPC_SERVER = new Guid.CLSID("CAE8D0E1-117B-11D5-924B-11C0F023E91C");

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {

  }

  @Override
  public void handshake() throws Exception {}

  @Override
  public void heartbeat() throws Exception {
    // Do nothing
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {}

  @Override
  public void transfer(final Event event) throws Exception {
    // Do nothing
  }

  @Override
  public void close() throws Exception {}
}
