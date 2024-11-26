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

package org.apache.tsfile.utils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;

public class WriteUtils {

  /**
   * A Tablet for the table-view insert interface may contain more than one device. This method
   * splits a Tablet by different deviceIds so that the caller can insert them device-by-device.
   *
   * @return (deviceId, endRowNum) pairs
   */
  public static List<Pair<IDeviceID, Integer>> splitTabletByDevice(Tablet tablet) {
    List<Pair<IDeviceID, Integer>> result = new ArrayList<>();
    IDeviceID lastDeviceID = null;
    for (int i = 0; i < tablet.getRowSize(); i++) {
      final IDeviceID currDeviceID = tablet.getDeviceID(i);
      if (!currDeviceID.equals(lastDeviceID)) {
        if (lastDeviceID != null) {
          result.add(new Pair<>(lastDeviceID, i));
        }
        lastDeviceID = currDeviceID;
      }
    }
    result.add(new Pair<>(lastDeviceID, tablet.getRowSize()));
    return result;
  }

  public static int compareStrings(String a, String b) {
    if (a == null && b == null) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    return a.compareTo(b);
  }
}
