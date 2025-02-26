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

import org.apache.iotdb.db.pipe.connector.protocol.opcua.OpcUaConnector;
import org.apache.iotdb.db.pipe.connector.util.sorter.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;
import com.sun.jna.platform.win32.COM.IUnknown;
import com.sun.jna.platform.win32.COM.Unknown;
import com.sun.jna.platform.win32.Guid;
import com.sun.jna.platform.win32.OaIdl;
import com.sun.jna.platform.win32.Ole32;
import com.sun.jna.platform.win32.OleAuto;
import com.sun.jna.platform.win32.Variant;
import com.sun.jna.platform.win32.WTypes;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_OPC_DA_CLSID_KEY;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAHeader.IID_IOPCItemMgt;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAHeader.IID_IOPCServer;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAHeader.IID_IOPCSyncIO;
import static org.apache.iotdb.db.pipe.connector.protocol.opcda.OPCDAHeader.IID_IUNKNOWN;

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
  private final Map<String, Integer> serverHandleMap = new HashMap<>();

  // Save it here to avoid memory leakage
  private WTypes.BSTR bstr;

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
        opcServer.addGroup(
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
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    OpcUaConnector.transferByTablet(
        tabletInsertionEvent, LOGGER, (tablet, isTableModel) -> transfer(tablet));
  }

  private void transfer(final Tablet tablet) {
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();
    final List<IMeasurementSchema> schemas = tablet.getSchemas();

    for (int i = 0; i < schemas.size(); ++i) {
      final String itemId =
          tablet.getDeviceId()
              + TsFileConstant.PATH_SEPARATOR
              + schemas.get(i).getMeasurementName();
      if (!serverHandleMap.containsKey(itemId)) {
        addItem(itemId, schemas.get(i).getType());
      }
      for (int j = tablet.getRowSize() - 1; j >= 0; --j) {
        if (Objects.isNull(tablet.getBitMaps())
            || Objects.isNull(tablet.getBitMaps()[i])
            || !tablet.getBitMaps()[i].isMarked(j)) {
          writeData(
              itemId, getTabletObjectValue4Opc(tablet.getValues()[i], j, schemas.get(i).getType()));
          break;
        }
      }
    }
  }

  private void addItem(final String itemId, final TSDataType type) {
    final OPCDAHeader.OPCITEMDEF[] itemDefs = new OPCDAHeader.OPCITEMDEF[1];
    itemDefs[0] = new OPCDAHeader.OPCITEMDEF();
    itemDefs[0].szAccessPath = new WString("");
    itemDefs[0].szItemID = new WString(itemId + "\0");
    itemDefs[0].bActive = 1;
    itemDefs[0].hClient = 0;
    itemDefs[0].dwBlobSize = 0;
    itemDefs[0].pBlob = Pointer.NULL;
    itemDefs[0].vtRequestedDataType = convertTsDataType2VariantType(type);
    itemDefs[0].wReserved = 0;
    itemDefs[0].write();

    final PointerByReference ppItemResults = new PointerByReference();
    final PointerByReference ppErrors = new PointerByReference();
    final int hr = itemMgt.addItems(1, itemDefs, ppItemResults, ppErrors);

    final Pointer pErrors = ppErrors.getValue();
    if (Objects.nonNull(pErrors)) {
      // Read errors
      final int[] errors =
          pErrors.getIntArray(0, 1); // Pick 1 element because only 1 element is added
      final int itemError = errors[0];

      try {
        if (itemError == WinError.S_OK.intValue()) {
          LOGGER.debug("Successfully added item {}.", itemId);
        } else {
          throw new PipeException(
              "Failed to add item "
                  + itemId
                  + ", opc error code: 0x"
                  + Integer.toHexString(itemError));
        }
      } finally {
        Ole32.INSTANCE.CoTaskMemFree(pErrors);
      }
    }

    if (hr != WinError.S_OK.intValue()) {
      throw new PipeException("Failed to add item, win error code: 0x" + Integer.toHexString(hr));
    }

    final Pointer pItemResults = ppItemResults.getValue();

    final OPCDAHeader.OPCITEMRESULT[] itemResults = new OPCDAHeader.OPCITEMRESULT[1];
    itemResults[0] = new OPCDAHeader.OPCITEMRESULT(pItemResults);
    itemResults[0].read();

    serverHandleMap.put(itemId, itemResults[0].hServer);
  }

  private void writeData(final String itemId, final Variant.VARIANT value) {
    final Pointer phServer = new Memory(Native.getNativeSize(int.class));
    phServer.write(0, new int[] {serverHandleMap.get(itemId)}, 0, 1);

    final PointerByReference ppErrors = new PointerByReference();
    final int hr = syncIO.write(1, phServer, value.getPointer(), ppErrors);
    // Free after write
    if (Objects.nonNull(bstr)) {
      OleAuto.INSTANCE.SysFreeString(bstr);
    }

    final Pointer pErrors = ppErrors.getValue();
    if (Objects.nonNull(pErrors)) {
      // Read error code array, each for a result
      final int[] errors =
          pErrors.getIntArray(0, 1); // Read 1 element because only 1 point is written
      final int itemError = errors[0];

      try {
        if (itemError != WinError.S_OK.intValue()) {
          throw new PipeException(
              "Failed to write "
                  + itemId
                  + ", value: "
                  + value
                  + ", opc error code: 0x"
                  + Integer.toHexString(itemError));
        }
      } finally {
        Ole32.INSTANCE.CoTaskMemFree(pErrors);
      }
    }

    if (hr != WinError.S_OK.intValue()) {
      throw new PipeException("Failed to write, win error code: 0x" + Integer.toHexString(hr));
    }
  }

  private short convertTsDataType2VariantType(final TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Variant.VT_BOOL;
      case INT32:
        return Variant.VT_I4;
      case INT64:
        return Variant.VT_I8;
      case DATE:
      case TIMESTAMP:
        return Variant.VT_DATE;
      case FLOAT:
        return Variant.VT_R4;
      case DOUBLE:
        return Variant.VT_R8;
      case TEXT:
      case STRING:
        return Variant.VT_BSTR;
      case BLOB:
        return Variant.VT_BLOB;
      default:
        throw new UnSupportedDataTypeException("UnSupported dataType " + dataType);
    }
  }

  private Variant.VARIANT getTabletObjectValue4Opc(
      final Object column, final int rowIndex, final TSDataType type) {
    final Variant.VARIANT value = new Variant.VARIANT();
    switch (type) {
      case BOOLEAN:
        value.setValue(
            Variant.VT_BOOL,
            ((boolean[]) column)[rowIndex] ? Variant.VARIANT_TRUE : Variant.VARIANT_FALSE);
        break;
      case INT32:
        value.setValue(Variant.VT_I4, new WinDef.LONG(((int[]) column)[rowIndex]));
        break;
      case DATE:
        value.setValue(
            Variant.VT_DATE, new OaIdl.DATE((Date.valueOf(((LocalDate[]) column)[rowIndex]))));
        break;
      case INT64:
        value.setValue(Variant.VT_I8, new WinDef.LONGLONG(((long[]) column)[rowIndex]));
        break;
      case TIMESTAMP:
        value.setValue(
            Variant.VT_DATE, new OaIdl.DATE(new java.util.Date(((long[]) column)[rowIndex])));
        break;
      case FLOAT:
        value.setValue(Variant.VT_R4, ((float[]) column)[rowIndex]);
        break;
      case DOUBLE:
        value.setValue(Variant.VT_R8, ((double[]) column)[rowIndex]);
        break;
      case TEXT:
      case STRING:
        bstr = OleAuto.INSTANCE.SysAllocString(((Binary[]) column)[rowIndex].toString());
        value.setValue(Variant.VT_BSTR, bstr);
        break;
      case BLOB:
      default:
        throw new UnSupportedDataTypeException("UnSupported dataType " + type);
    }
    return value;
  }

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
    // Unload COM
    Ole32.INSTANCE.CoUninitialize();
  }
}
