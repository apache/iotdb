/**
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
package org.apache.iotdb.db.utils;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;

public class SyncUtils {

  private static final String IP_SEPARATOR = "\\.";

  private static String[] snapshotPaths = SyncSenderDescriptor.getInstance()
      .getConfig().getSnapshotPaths();

  private SyncUtils() {
  }

  /**
   * This method is to get a snapshot file seriesPath according to a tsfile seriesPath. Due to
   * multiple directories, it's necessary to make a snapshot in the same disk. It's used by sync
   * sender.
   */
  public static String getSnapshotFilePath(String filePath) {
    String[] name;
    String relativeFilePath;
    String os = System.getProperty("os.name");
    if (os.toLowerCase().startsWith("windows")) {
      name = filePath.split(File.separator + File.separator);
      relativeFilePath = name[name.length - 2] + File.separator + name[name.length - 1];
    } else {
      name = filePath.split(File.separator);
      relativeFilePath = name[name.length - 2] + File.separator + name[name.length - 1];
    }
    String bufferWritePath = name[0];
    for (int i = 1; i < name.length - 2; i++) {
      bufferWritePath = bufferWritePath + File.separatorChar + name[i];
    }
    for (String snapshotPath : snapshotPaths) {
      if (snapshotPath.startsWith(bufferWritePath)) {
        if (!new File(snapshotPath).exists()) {
          new File(snapshotPath).mkdir();
        }
        if (snapshotPath.length() > 0
            && snapshotPath.charAt(snapshotPath.length() - 1) != File.separatorChar) {
          snapshotPath = snapshotPath + File.separatorChar;
        }
        return snapshotPath + relativeFilePath;
      }
    }
    return null;
  }

  /**
   * Verify sending list is empty or not It's used by sync sender.
   */
  public static boolean isEmpty(Map<String, Set<String>> sendingFileList) {
    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verify IP address with IP white list which contains more than one IP segment. It's used by sync
   * sender.
   */
  public static boolean verifyIPSegment(String ipWhiteList, String ipAddress) {
    String[] ipSegments = ipWhiteList.split(",");
    for (String IPsegment : ipSegments) {
      int subnetMask = Integer.parseInt(IPsegment.substring(IPsegment.indexOf('/') + 1));
      IPsegment = IPsegment.substring(0, IPsegment.indexOf('/'));
      if (verifyIP(IPsegment, ipAddress, subnetMask)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Verify IP address with IP segment.
   */
  private static boolean verifyIP(String ipSegment, String ipAddress, int subnetMark) {
    String ipSegmentBinary;
    String ipAddressBinary;
    String[] ipSplits = ipSegment.split(IP_SEPARATOR);
    DecimalFormat df = new DecimalFormat("00000000");
    StringBuilder ipSegmentBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipSegmentBuilder.append(df.format(
          Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipSegmentBinary = ipSegmentBuilder.toString();
    ipSegmentBinary = ipSegmentBinary.substring(0, subnetMark);
    ipSplits = ipAddress.split(IP_SEPARATOR);
    StringBuilder ipAddressBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipAddressBuilder.append(df.format(
          Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipAddressBinary = ipAddressBuilder.toString();
    ipAddressBinary = ipAddressBinary.substring(0, subnetMark);
    return ipAddressBinary.equals(ipSegmentBinary);
  }
}
