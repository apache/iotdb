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
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

public class EncryptDBUtils {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  private static final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  private static final Logger LOGGER = LoggerFactory.getLogger(EncryptDBUtils.class);

  private static volatile EncryptParameter defaultFirstEncryptParam;
  private static final FileEncryptionParameterProvider LEGACY_PROVIDER =
      (file, database, legacy) -> legacy;

  private static class ProviderHolder {
    private static final FileEncryptionParameterProvider PROVIDER = loadProvider();
  }

  private static FileEncryptionParameterProvider loadProvider() {
    Iterator<FileEncryptionParameterProvider> providers =
        ServiceLoader.load(FileEncryptionParameterProvider.class).iterator();
    if (!providers.hasNext()) {
      return LEGACY_PROVIDER;
    }
    FileEncryptionParameterProvider provider = providers.next();
    if (providers.hasNext()) {
      throw new IllegalStateException(
          StorageEngineMessages
              .EXCEPTION_MULTIPLE_FILE_ENCRYPTION_PARAMETER_PROVIDERS_ARE_CONFIGURED_5E7DDA5B);
    }
    return provider;
  }

  public static EncryptParameter createEncryptParameterForNewFile(
      File file, String database, EncryptParameter legacyParameter) {
    EncryptParameter parameter =
        ProviderHolder.PROVIDER.createForNewFile(file, database, legacyParameter);
    if (parameter == null && ProviderHolder.PROVIDER != LEGACY_PROVIDER) {
      throw new IllegalStateException(
          StorageEngineMessages
              .EXCEPTION_FILE_ENCRYPTION_PARAMETER_PROVIDER_RETURNED_NULL_15C41723);
    }
    return parameter;
  }

  public static boolean requiresFileRewrite() {
    return ProviderHolder.PROVIDER.requiresFileRewrite();
  }

  public static RestorableTsFileIOWriter createRestorableWriterForNewFile(
      File file, String database) throws IOException {
    return createRestorableWriterForNewFile(
        file, database, getFirstEncryptParamFromDatabase(database));
  }

  public static RestorableTsFileIOWriter createRestorableWriterForNewFile(
      File file, String database, EncryptParameter legacyParameter) throws IOException {
    EncryptParameter parameter = createEncryptParameterForNewFile(file, database, legacyParameter);
    try {
      return new RestorableTsFileIOWriter(file, parameter);
    } finally {
      closeFileEncryptionParameter(parameter);
    }
  }

  public static TsFileIOWriter createWriterForNewFile(File file, String database)
      throws IOException {
    EncryptParameter parameter =
        createEncryptParameterForNewFile(
            file, database, getFirstEncryptParamFromDatabase(database));
    try {
      return new TsFileIOWriter(file, parameter);
    } finally {
      closeFileEncryptionParameter(parameter);
    }
  }

  public static void closeFileEncryptionParameter(EncryptParameter parameter) {
    if (parameter != null && parameter.isTdePageAead()) {
      parameter.close();
    }
  }

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
