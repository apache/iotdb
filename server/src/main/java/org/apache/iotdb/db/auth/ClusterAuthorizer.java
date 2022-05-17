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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.rpc.ConfigNodeConnectionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.util.List;

public class ClusterAuthorizer {

  public static TAuthorizerResp checkPath(String username, List<String> allPath, int permission) {
    TCheckUserPrivilegesReq req = new TCheckUserPrivilegesReq(username, allPath, permission);
    ConfigNodeClient configNodeClient = null;
    TAuthorizerResp status = null;
    try {
      configNodeClient = new ConfigNodeClient();
      // Send request to some API server
      status = configNodeClient.checkUserPrivileges(req);
    } catch (IoTDBConnectionException e) {
      throw new ConfigNodeConnectionException("Couldn't connect config node");
    } finally {
      if (configNodeClient != null) {
        configNodeClient.close();
      }
      if (status == null) {
        status = new TAuthorizerResp();
      }
    }
    return status;
  }

  /** Check the user */
  public static TAuthorizerResp checkUser(String username, String password) {
    TLoginReq req = new TLoginReq(username, password);
    TAuthorizerResp status = null;
    ConfigNodeClient configNodeClient = null;
    try {
      configNodeClient = new ConfigNodeClient();
      // Send request to some API server
      status = configNodeClient.login(req);
    } catch (IoTDBConnectionException e) {
      throw new ConfigNodeConnectionException("Couldn't connect config node");
    } finally {
      if (configNodeClient != null) {
        configNodeClient.close();
      }
      if (status == null) {
        status = new TAuthorizerResp();
      }
    }
    return status;
  }
}
