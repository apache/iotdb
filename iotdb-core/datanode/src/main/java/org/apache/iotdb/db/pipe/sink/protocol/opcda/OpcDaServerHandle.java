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

import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTreeModelTabletEventSorter;
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

import java.io.Closeable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.pipe.sink.protocol.opcda.OpcDaHeader.IID_IOPCItemMgt;
import static org.apache.iotdb.db.pipe.sink.protocol.opcda.OpcDaHeader.IID_IOPCServer;
import static org.apache.iotdb.db.pipe.sink.protocol.opcda.OpcDaHeader.IID_IOPCSyncIO;
import static org.apache.iotdb.db.pipe.sink.protocol.opcda.OpcDaHeader.IID_IUNKNOWN;

public class OpcDaServerHandle implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpcDaServerHandle.class);

  private final OpcDaHeader.IOPCServer opcServer;
  private final OpcDaHeader.IOPCItemMgt itemMgt;
  private final OpcDaHeader.IOPCSyncIO syncIO;
  private final Map<String, Integer> serverHandleMap = new ConcurrentHashMap<>();
  private final Map<String, Long> serverTimestampMap = new ConcurrentHashMap<>();

  // Save it here to avoid memory leakage
  private WTypes.BSTR bstr;

  OpcDaServerHandle(String clsOrProgID) {
    final Guid.CLSID CLSID_OPC_SERVER = new Guid.CLSID(clsOrProgID);

    Ole32.INSTANCE.CoInitializeEx(null, Ole32.COINIT_MULTITHREADED);
    final PointerByReference ppvServer = new PointerByReference();

    WinNT.HRESULT hr =
        Ole32.INSTANCE.CoCreateInstance(CLSID_OPC_SERVER, null, 0x17, IID_IOPCServer, ppvServer);

    if (hr.intValue() != WinError.S_OK.intValue()) {
      throw new PipeException(
          "Failed to connect to server, error code: 0x" + Integer.toHexString(hr.intValue()));
    }

    opcServer = new OpcDaHeader.IOPCServer(ppvServer.getValue());

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

    itemMgt = new OpcDaHeader.IOPCItemMgt(ppvItemMgt.getValue());

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
    syncIO = new OpcDaHeader.IOPCSyncIO(ppvSyncIO.getValue());
  }

  static String getClsIDFromProgID(final String progID) {
    // To receive CLSID struct
    final Guid.CLSID.ByReference pclsid = new Guid.CLSID.ByReference();

    final WinNT.HRESULT hr = Ole32.INSTANCE.CLSIDFromProgID(progID, pclsid);

    if (hr.intValue() == WinError.S_OK.intValue()) { // S_OK = 0
      // Format CLSID (like "{CAE8D0E1-117B-11D5-924B-11C0F023E91C}")
      final String clsidStr =
          String.format(
              "%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
              pclsid.Data1,
              pclsid.Data2,
              pclsid.Data3,
              pclsid.Data4[0],
              pclsid.Data4[1],
              pclsid.Data4[2],
              pclsid.Data4[3],
              pclsid.Data4[4],
              pclsid.Data4[5],
              pclsid.Data4[6],
              pclsid.Data4[7]);
      LOGGER.info("Successfully converted progID {} to CLSID: {{}}", progID, clsidStr);
      return clsidStr;
    } else {
      throw new PipeException(
          "Error: ProgID is invalid or unregistered, (HRESULT=0x"
              + Integer.toHexString(hr.intValue())
              + ")");
    }
  }

  void transfer(final Tablet tablet) {
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
        if (!tablet.isNull(j, i)) {
          if (serverTimestampMap.get(itemId) <= tablet.getTimestamp(j)) {
            writeData(
                itemId,
                getTabletObjectValue4Opc(tablet.getValues()[i], j, schemas.get(i).getType()));
            serverTimestampMap.put(itemId, tablet.getTimestamp(j));
          }
          break;
        }
      }
    }
  }

  private void addItem(final String itemId, final TSDataType type) {
    final OpcDaHeader.OPCITEMDEF[] itemDefs = new OpcDaHeader.OPCITEMDEF[1];
    itemDefs[0] = new OpcDaHeader.OPCITEMDEF();
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

    final OpcDaHeader.OPCITEMRESULT[] itemResults = new OpcDaHeader.OPCITEMRESULT[1];
    itemResults[0] = new OpcDaHeader.OPCITEMRESULT(pItemResults);
    itemResults[0].read();

    serverHandleMap.put(itemId, itemResults[0].hServer);
    serverTimestampMap.put(itemId, Long.MIN_VALUE);
  }

  private void writeData(final String itemId, final Variant.VARIANT value) {
    final Pointer phServer = new Memory(Native.getNativeSize(int.class));
    phServer.write(0, new int[] {serverHandleMap.get(itemId)}, 0, 1);

    final PointerByReference ppErrors = new PointerByReference();
    final int hr = syncIO.write(1, phServer, value.getPointer(), ppErrors);
    // Free after write
    if (Objects.nonNull(bstr)) {
      OleAuto.INSTANCE.SysFreeString(bstr);
      bstr = null;
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
        // Note that "Variant" does not support "VT_BLOB" data, and not all the DA server
        // support this, thus we use "VT_BSTR" to substitute
      case BLOB:
      case OBJECT:
        return Variant.VT_BSTR;
      default:
        throw new UnSupportedDataTypeException("UnSupported dataType " + dataType);
    }
  }

  private Variant.VARIANT getTabletObjectValue4Opc(
      final Object column, final int rowIndex, final TSDataType type) {
    final Variant.VARIANT value = new Variant.VARIANT();
    switch (type) {
      case BOOLEAN:
        value.setValue(Variant.VT_BOOL, new OaIdl.VARIANT_BOOL(((boolean[]) column)[rowIndex]));
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
      case BLOB:
      case OBJECT:
        bstr = OleAuto.INSTANCE.SysAllocString(((Binary[]) column)[rowIndex].toString());
        value.setValue(Variant.VT_BSTR, bstr);
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported dataType " + type);
    }
    return value;
  }

  @Override
  public void close() {
    // Help gc
    serverTimestampMap.clear();
    serverHandleMap.clear();

    // Release resource
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
