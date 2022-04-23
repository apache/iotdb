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
package org.apache.iotdb.tsfile.read.common;

import org.junit.Test;

public class DeviceIdTest {
  @Test
  public void testSplit() throws Exception {
    String test1 = "root.sg.`select`";
    String test2 = "root.sg.`select```";
    String test3 = "root.sg.a";
    DeviceId deviceId = new DeviceId("1");
    printStringArray(deviceId.splitDeviceIdToNodes(test1));
    printStringArray(deviceId.splitDeviceIdToNodes(test2));
    printStringArray(deviceId.splitDeviceIdToNodes(test3));
  }

  private void printStringArray(String[] array) {
    for (String s : array) {
      System.out.print(s + " ");
    }
    System.out.println();
  }
}
