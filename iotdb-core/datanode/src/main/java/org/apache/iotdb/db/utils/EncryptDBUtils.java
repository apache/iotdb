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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EncryptDBUtils {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  private static final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  private static final Logger LOGGER = LoggerFactory.getLogger(EncryptDBUtils.class);

  private static volatile EncryptParameter defaultFirstEncryptParam;

  public static EncryptParameter getFirstEncryptParamFromDatabase(String database) {
    if (database == null || database.isEmpty()) {
      return new EncryptParameter(tsFileConfig.getEncryptType(), tsFileConfig.getEncryptKey());
    }
    for (Map.Entry<String, EncryptParameter> entry : conf.getTSFileDBToEncryptMap().entrySet()) {
      if (database.trim().equals(entry.getKey().trim())) {
        return entry.getValue();
      }
    }
    return getDefaultFirstEncryptParam();
  }

  public static EncryptParameter getSecondEncryptParamFromDatabase(String database) {
    return EncryptUtils.getEncryptParameter(getFirstEncryptParamFromDatabase(database));
  }

  public static EncryptParameter getFirstEncryptParamFromTSFilePath(String tsFilePath) {
    if (tsFilePath == null || tsFilePath.isEmpty()) {
      return new EncryptParameter(tsFileConfig.getEncryptType(), tsFileConfig.getEncryptKey());
    }
    // Use system-specific file separator
    String separator = java.io.File.separator;
    String[] pathSegments = tsFilePath.split(separator.equals("\\") ? "\\\\" : separator);
    for (int i = pathSegments.length - 1; i >= 0; i--) {
      if ("sequence".equals(pathSegments[i]) || "unsequence".equals(pathSegments[i])) {
        if (i + 1 < pathSegments.length) {
          return getFirstEncryptParamFromDatabase(pathSegments[i + 1]);
        }
      }
    }
    return getDefaultFirstEncryptParam();
  }

  public static EncryptParameter getDefaultFirstEncryptParam() {
    if (defaultFirstEncryptParam == null) {
      synchronized (EncryptDBUtils.class) {
        if (defaultFirstEncryptParam == null) {
          defaultFirstEncryptParam =
              new EncryptParameter(tsFileConfig.getEncryptType(), tsFileConfig.getEncryptKey());
        }
      }
    }
    return defaultFirstEncryptParam;
  }
}
