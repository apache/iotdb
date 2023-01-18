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

package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class DataNodeEndPoints {

  private DataNodeEndPoints() {
    // forbidding instantiation
  }

  public static final TEndPoint LOCAL_HOST_DATA_BLOCK_ENDPOINT =
      new TEndPoint(
          IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
          IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());

  public static final TEndPoint LOCAL_HOST_INTERNAL_ENDPOINT =
      new TEndPoint(
          IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
          IoTDBDescriptor.getInstance().getConfig().getInternalPort());

  public static boolean isSameNode(TEndPoint endPoint) {
    return endPoint.equals(LOCAL_HOST_DATA_BLOCK_ENDPOINT);
  }
}
