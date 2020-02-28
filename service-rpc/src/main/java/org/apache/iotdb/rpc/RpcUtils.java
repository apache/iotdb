/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.rpc;

import java.lang.reflect.Proxy;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class RpcUtils {

  public static TSIService.Iface newSynchronizedClient(TSIService.Iface client) {
    return (TSIService.Iface) Proxy.newProxyInstance(RpcUtils.class.getClassLoader(),
        new Class[]{TSIService.Iface.class}, new SynchronizedHandler(client));
  }

  /**
   * verify success.
   *
   * @param status -status
   */
  public static void verifySuccess(TSStatus status) throws IoTDBRPCException {
    if (status.getStatusType().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IoTDBRPCException(String.format("%d: %s", status.getStatusType().getCode(), status.getStatusType().getMessage()));
    }
  }

}
