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
package org.apache.iotdb.library.util;

import org.apache.iotdb.session.Session;

// IoTDB 0.12.0 session interface
public class IoTDBSession_0_12_0 {
  Session session;

  public IoTDBSession_0_12_0(String host, int rpcPort) {
    session = new Session(host, rpcPort);
  }

  public IoTDBSession_0_12_0(String host, String rpcPort, String username, String password) {
    session = new Session(host, rpcPort, username, password);
  }

  public IoTDBSession_0_12_0(String host, int rpcPort, String username, String password) {
    session = new Session(host, rpcPort, username, password);
  }

  public static void main(String[] args) {
    IoTDBSession_0_12_0 Session = new IoTDBSession_0_12_0("127.0.0.1", "6667", "root", "root");
  }
}
