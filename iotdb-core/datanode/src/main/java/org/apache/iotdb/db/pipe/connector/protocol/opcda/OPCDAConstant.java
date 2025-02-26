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

import com.sun.jna.platform.win32.Guid;

/** Some constants in OPC DA Standard */
public class OPCDAConstant {

  /////////////////////////////// IID of OPC DA Interfaces ///////////////////////////////

  // IOPCServer
  static final Guid.IID IID_IOPCServer = new Guid.IID("39C13A4D-011E-11D0-9675-0020AFD8ADB3");

  // IOPCItemMgt
  static final Guid.IID IID_IOPCItemMgt = new Guid.IID("39C13A54-011E-11D0-9675-0020AFD8ADB3");

  // IOPCSyncIO
  static final Guid.IID IID_IOPCSyncIO = new Guid.IID("39C13A52-011E-11D0-9675-0020AFD8ADB3");

  // IUnknown
  static final Guid.IID IID_IUNKNOWN = new Guid.IID("00000000-0000-0000-C000-000000000046");

  /////////////////////////////// Data types ///////////////////////////////

  // BOOLEAN
  static final int VT_BOOL = 0x0000000b;

  // FLOAT
  static final int VT_R4 = 0x00000004;

  // DOUBLE
  static final int VT_R8 = 0x00000005;

  // INT32
  static final int VT_I4 = 0x00000003;

  // INT64
  static final int VT_I8 = 0x00000014;

  // DATE
  static final int VT_DATE = 0x00000007;

  // TEXT / STRING
  static final int VT_BSTR = 0x00000008;
}
