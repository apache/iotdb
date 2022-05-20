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

package org.apache.iotdb.db.engine.trigger.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HTTPConnectionPool {

  private static volatile PoolingHttpClientConnectionManager clientConnectionManager;

  private HTTPConnectionPool() {}

  public static PoolingHttpClientConnectionManager getInstance() {
    if (clientConnectionManager == null) {
      synchronized (HTTPConnectionPool.class) {
        if (clientConnectionManager == null) {
          clientConnectionManager = new PoolingHttpClientConnectionManager();
          // Set the max number of connections
          clientConnectionManager.setMaxTotal(
              IoTDBDescriptor.getInstance().getConfig().getTriggerForwardHTTPPoolSize());
          // Set the maximum number of connections per host and the specified number of connections
          // per website, which will not affect the access of other websites
          clientConnectionManager.setDefaultMaxPerRoute(
              IoTDBDescriptor.getInstance().getConfig().getTriggerForwardHTTPPOOLMaxPerRoute());
        }
      }
    }
    return clientConnectionManager;
  }
}
