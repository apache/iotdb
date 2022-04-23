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

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeviceId {

  private String[] nodes;

  public DeviceId(String deviceId) {}

  public DeviceId(String[] nodes) {}

  public static void main(String[] args) {
    DeviceId deviceId = new DeviceId(new String[] {"1"});
    String test = "1.1";
    System.out.println(test.split("\\.")[1]);
  }

  /**
   * @param deviceId
   * @return
   */
  public String[] splitDeviceIdToNodes(String deviceId) throws Exception {
    if (!deviceId.contains(String.valueOf(TsFileConstant.BACK_QUOTE))) {
      return deviceId.split("\\.");
    }
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    int endIndex;
    int length = deviceId.length();
    for (int i = 0; i < length; i++) {
      if (deviceId.charAt(i) == TsFileConstant.BACK_QUOTE) {
        startIndex = i + 1;
        endIndex = deviceId.indexOf(TsFileConstant.BACK_QUOTE, startIndex);
        while (endIndex != -1 && endIndex != length - 1) {
          // end at a single backquote
          if (deviceId.charAt(endIndex + 1) == TsFileConstant.BACK_QUOTE) {
            endIndex = deviceId.indexOf(TsFileConstant.BACK_QUOTE, endIndex + 2);
          }
        }
        // replace `` with ` in a quoted identifier
        String node = deviceId.substring(startIndex, endIndex).replace("``", "`");
        if (node.isEmpty()) {}

        nodes.add(node);
        i = endIndex + 1;
        startIndex = endIndex + 1;
      } else if (deviceId.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        String node = deviceId.substring(startIndex, i);
        if (node.isEmpty()) {}

        nodes.add(node);
        startIndex = i + 1;
      }
    }
    return nodes.toArray(new String[0]);
  }

  public String[] getNodes() {
    return this.nodes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeviceId that = (DeviceId) o;
    return Arrays.equals(that.getNodes(), this.nodes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(nodes);
  }
}
