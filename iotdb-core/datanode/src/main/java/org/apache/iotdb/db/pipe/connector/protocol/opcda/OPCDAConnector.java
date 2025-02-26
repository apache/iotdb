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
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.sun.jna.platform.win32.COM.IUnknown;
import com.sun.jna.platform.win32.COM.Unknown;
import com.sun.jna.platform.win32.Guid;
import com.sun.jna.platform.win32.Ole32;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAConstant.IID_IOPCItemMgt;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAConstant.IID_IOPCServer;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAConstant.IID_IOPCSyncIO;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAConstant.IID_IUNKNOWN;

/**
 * Send data in IoTDB based on Opc Da protocol, using JNA. All data are converted into tablets, and
 * then push the newest value to the <b>local COM</b> server in another process.
 */
@TreeModel
public class OPCDAConnector implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(OPCDAConnector.class);
  private final PointerByReference ppvServer = new PointerByReference();
  private OPCDAHeader.IOPCServer opcServer;
  private OPCDAHeader.IOPCItemMgt itemMgt;
  private OPCDAHeader.IOPCSyncIO syncIO;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator.validateSynonymAttributes(
        Collections.singletonList(CONNECTOR_OPC_DA_CLSID_KEY),
        Collections.singletonList(SINK_OPC_DA_CLSID_KEY),
        true);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final Guid.CLSID CLSID_OPC_SERVER =
        new Guid.CLSID(
            parameters.getStringByKeys(CONNECTOR_OPC_DA_CLSID_KEY, SINK_OPC_DA_CLSID_KEY));

    Ole32.INSTANCE.CoInitializeEx(null, Ole32.COINIT_MULTITHREADED);
    final PointerByReference ppvServer = new PointerByReference();

    WinNT.HRESULT hr =
        Ole32.INSTANCE.CoCreateInstance(CLSID_OPC_SERVER, null, 0x17, IID_IOPCServer, ppvServer);

    if (hr.intValue() != WinError.S_OK.intValue()) {
      throw new PipeException(
          "Failed to connect to server, error code: 0x" + Integer.toHexString(hr.intValue()));
    }

    opcServer = new OPCDAHeader.IOPCServer(ppvServer.getValue());

    // 3. Create group
    final PointerByReference phServerGroup = new PointerByReference();
    final PointerByReference phOPCGroup = new PointerByReference();
    final IntByReference pRevisedUpdateRate = new IntByReference();
    final int hr2 =
        opcServer.AddGroup(
            "",
            true,
            1000,
            0,
            null,
            null,
            0,
            phServerGroup,
            pRevisedUpdateRate,
            new Guid.GUID.ByReference(IID_IUNKNOWN.getPointer()),
            phOPCGroup);

    if (hr2 == WinError.S_OK.intValue()) {
      LOGGER.info(
          "Create group successfully! Server handle: {}, update rate: {} ms",
          phServerGroup.getValue(),
          pRevisedUpdateRate.getValue());
    } else {
      throw new PipeException(
          "Failed to create group，error code: 0x" + Integer.toHexString(hr.intValue()));
    }

    final IUnknown groupUnknown = new Unknown(phOPCGroup.getValue());

    // 4. Acquire IOPCItemMgt interface (To create Item)
    final PointerByReference ppvItemMgt = new PointerByReference();
    hr =
        groupUnknown.QueryInterface(
            new Guid.REFIID(new Guid.GUID.ByReference(IID_IOPCItemMgt).getPointer()), ppvItemMgt);
    if (hr.intValue() == WinError.S_OK.intValue()) {
      LOGGER.info("Acquire IOPCItemMgt successfully! Interface address: {}", ppvItemMgt.getValue());
    } else {
      throw new PipeException(
          "Failed to acquire IOPCItemMgt, error code: 0x" + Integer.toHexString(hr.intValue()));
    }

    itemMgt = new OPCDAHeader.IOPCItemMgt(ppvItemMgt.getValue());

    // 5. Acquire IOPCSyncIO Interface
    PointerByReference ppvSyncIO = new PointerByReference();
    hr =
        groupUnknown.QueryInterface(
            new Guid.REFIID(new Guid.GUID.ByReference(IID_IOPCSyncIO).getPointer()), ppvSyncIO);
    if (hr.intValue() == WinError.S_OK.intValue()) {
      LOGGER.info("Acquire IOPCSyncIO successfully! Interface address: {}", ppvSyncIO.getValue());
    } else {
      throw new PipeException(
          "Failed to acquire IOPCSyncIO, error code: 0x" + Integer.toHexString(hr.intValue()));
    }
    syncIO = new OPCDAHeader.IOPCSyncIO(ppvSyncIO.getValue());
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
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {}

  @Override
  public void transfer(final Event event) throws Exception {
    // Do nothing
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(ppvServer.getValue())) {
      Ole32.INSTANCE.CoTaskMemFree(ppvServer.getValue());
    }
    if (Objects.nonNull(syncIO)) {
      syncIO.Release();
    }
    if (Objects.nonNull(itemMgt)) {
      itemMgt.Release();
    }
    if (Objects.nonNull(opcServer)) {
      opcServer.Release();
    }
    // 卸载 COM 库
    Ole32.INSTANCE.CoUninitialize();
  }
}
