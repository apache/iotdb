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

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.WString;
import com.sun.jna.platform.win32.COM.Unknown;
import com.sun.jna.platform.win32.Guid;
import com.sun.jna.platform.win32.Variant;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

import java.util.Arrays;
import java.util.List;

/** We define the OPC DA Classes and interfaces here like C's .h file. */
public class OpcDaHeader {
  // IOPCServer
  static final Guid.IID IID_IOPCServer = new Guid.IID("39C13A4D-011E-11D0-9675-0020AFD8ADB3");

  // IOPCItemMgt
  static final Guid.IID IID_IOPCItemMgt = new Guid.IID("39C13A54-011E-11D0-9675-0020AFD8ADB3");

  // IOPCSyncIO
  static final Guid.IID IID_IOPCSyncIO = new Guid.IID("39C13A52-011E-11D0-9675-0020AFD8ADB3");

  // IUnknown
  static final Guid.IID IID_IUNKNOWN = new Guid.IID("00000000-0000-0000-C000-000000000046");

  public static class IOPCServer extends Unknown {
    public IOPCServer(final Pointer p) {
      super(p);
    }

    // /* [string][in] */ LPCWSTR szName,
    // /* [in] */ BOOL bActive,
    // /* [in] */ DWORD dwRequestedUpdateRate,
    // /* [in] */ OPCHANDLE hClientGroup,
    // /* [in][unique] */ LONG *pTimeBias,
    // /* [in][unique] */ FLOAT *pPercentDeadband,
    // /* [in] */ DWORD dwLCID,
    // /* [out] */ OPCHANDLE *phServerGroup,
    // /* [out] */ DWORD *pRevisedUpdateRate,
    // /* [in] */ REFIID riid,
    // /* [iid_is][out] */ LPUNKNOWN *ppUnk) = 0;
    public int addGroup(
        final String szName, // Group name ("" means auto)
        final boolean bActive, // Whether to activate the group
        final int dwRequestedUpdateRate, // The update rate of request (ms)
        final int hClientGroup, // The handle of client group
        final Pointer pTimeBias, // Time zone bias
        final Pointer pPercentDeadband, // Dead band
        final int dwLCID, // Region ID
        final PointerByReference phServerGroup, // Server group handler
        final IntByReference pRevisedUpdateRate, // Real update rate
        final Guid.GUID.ByReference riid, // Interface IID
        final PointerByReference ppUnk // The OPC Group pointer returned
        ) {
      // Convert Java string into COM "bstr"
      final WString wName = new WString(szName);

      return this._invokeNativeInt(
          3,
          new Object[] {
            this.getPointer(),
            wName,
            bActive ? 1 : 0,
            dwRequestedUpdateRate,
            hClientGroup,
            pTimeBias,
            pPercentDeadband,
            dwLCID,
            phServerGroup,
            pRevisedUpdateRate,
            riid != null ? riid.getPointer() : null,
            ppUnk
          });
    }
  }

  // IOPCItemMgt(
  // /* [in] */ DWORD dwCount,
  // /* [in] */ OPCITEMDEF *pItemArray,
  // /* [out] */ OPCITEMRESULT **ppAddResults,
  // /* [out] */ HRESULT **ppErrors) = 0;
  public static class IOPCItemMgt extends Unknown {
    public IOPCItemMgt(final Pointer p) {
      super(p);
    }

    public int addItems(
        final int dwCount, // Data count
        final OPCITEMDEF[] pItemArray, // Items array to create
        final PointerByReference pResults, // Results' handles
        final PointerByReference pErrors // Error's pointers
        ) {
      return this._invokeNativeInt(
          3, new Object[] {this.getPointer(), dwCount, pItemArray, pResults, pErrors});
    }
  }

  public static class IOPCSyncIO extends Unknown {
    public IOPCSyncIO(final Pointer p) {
      super(p);
    }

    // /* [in] */ DWORD dwCount,
    // /* [size_is][in] */ OPCHANDLE *phServer,
    // /* [size_is][in] */ VARIANT *pItemValues,
    // /* [size_is][size_is][out] */ HRESULT **ppErrors) = 0;
    public int write(
        final int dwCount, // Data count
        final Pointer phServer, // Server handles of items
        final Pointer pItemValues, // Values of items
        final PointerByReference pErrors // Error codes
        ) {
      return this._invokeNativeInt(
          4,
          new Object[] { // Write is the 4th method in vtable
            this.getPointer(), dwCount, phServer, pItemValues, pErrors
          });
    }
  }

  // /* [string] */ LPWSTR szAccessPath;
  // /* [string] */ LPWSTR szItemID;
  // BOOL bActive;
  // OPCHANDLE hClient;
  // DWORD dwBlobSize;
  // /* [size_is] */ BYTE *pBlob;
  // VARTYPE vtRequestedDataType;
  // WORD wReserved;
  public static class OPCITEMDEF extends Structure {
    public WString szAccessPath = new WString(""); // Access path (Usually empty)
    public WString szItemID; // Item ID（Like "Channel1.Device1.Tag1"）
    public int bActive; // Whether to activate this item（TRUE=1, FALSE=0）
    public int hClient; // Client handle, Used in async callback and remove item
    public int dwBlobSize; // BLOB size
    public Pointer pBlob; // BLOB's pointer
    public short vtRequestedDataType = Variant.VT_UNKNOWN; // Requested datatype
    public short wReserved; // Reserved

    // As C structure
    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "szAccessPath",
          "szItemID",
          "bActive",
          "hClient",
          "dwBlobSize",
          "pBlob",
          "vtRequestedDataType",
          "wReserved");
    }
  }

  // OPCHANDLE hServer;
  // VARTYPE vtCanonicalDataType;
  // WORD wReserved;
  // DWORD dwAccessRights;
  // DWORD dwBlobSize;
  // /* [size_is] */ BYTE *pBlob;
  public static class OPCITEMRESULT extends Structure {
    public int hServer; // Server handle, Used to write
    public short vtCanonicalDataType; // Data type (like Variant.VT_R8)
    public short wReserved; // Reserved word
    public int dwAccessRights; // Access right
    public int dwBlobSize; // BLOB size
    public Pointer pBlob; // BLOB pointer

    public OPCITEMRESULT(final Pointer pointer) {
      super(pointer);
    }

    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "hServer", "vtCanonicalDataType", "wReserved", "dwAccessRights", "dwBlobSize", "pBlob");
    }
  }
}
