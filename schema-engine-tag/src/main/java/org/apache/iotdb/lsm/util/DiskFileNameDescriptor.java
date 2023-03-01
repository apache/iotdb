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
package org.apache.iotdb.lsm.util;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaRegionConstant;

public class DiskFileNameDescriptor {

  public static Integer getTmpFlushFileID(String fileName) {
    return Integer.parseInt(
        fileName
            .substring(fileName.lastIndexOf("-") + 1)
            .replaceFirst(TagSchemaRegionConstant.TMP, ""));
  }

  public static String generateFlushFileName(String flushFilePrefix, int level, int fileID) {
    return flushFilePrefix + "-" + level + "-" + fileID;
  }

  public static String generateTmpFlushFileName(String flushFileName) {
    return flushFileName + TagSchemaRegionConstant.TMP;
  }

  public static String generateDeleteFlushFileName(String flushFilePrefix, int level, int fileID) {
    return flushFilePrefix + "-" + TagSchemaRegionConstant.DELETE + "-" + level + "-" + fileID;
  }

  public static String getFlushDeleteFileNameFromFlushFileName(String flushFileName) {
    String[] split = flushFileName.split("-");
    StringBuilder prefix = new StringBuilder();
    for (int i = 0; i < split.length - 2; i++) {
      prefix.append(split[i]);
    }
    StringBuilder suffix = new StringBuilder();
    suffix.append(split[split.length - 2]).append("-").append(split[split.length - 1]);
    return prefix + "-" + TagSchemaRegionConstant.DELETE + "-" + suffix;
  }
}
