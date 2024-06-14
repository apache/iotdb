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

package org.apache.iotdb.itbase.env;

public interface BaseNodeWrapper {

  void createNodeDir();

  void createLogDir();

  void destroyDir();

  void start();

  void stop();

  void stopForcibly();

  boolean isAlive();

  String getIp();

  int getPort();

  int getMetricPort();

  String getId();

  String getIpAndPortString();

  /**
   * Perform jstack on the process corresponding to the wrapper, and use logger to output the
   * results.
   */
  void executeJstack();

  /**
   * Perform jstack on the process corresponding to the wrapper, and output the results to a file in
   * the log directory.
   *
   * @param testCaseName the name of test case
   */
  void executeJstack(String testCaseName);

  long getPid();
}
