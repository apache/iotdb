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

package org.apache.iotdb.commons.file;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.FileUtils;

import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SystemPropertiesFileHandler {
  private static Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesFileHandler.class);

  private final File formalFile;
  private final File tmpFile;
  //    private ImmutableProperties cache;
  ReadWriteLock lock = new ReentrantReadWriteLock();

  private SystemPropertiesFileHandler(String filePath) {
    this.formalFile = SystemFileFactory.INSTANCE.getFile(filePath);
    this.tmpFile = SystemFileFactory.INSTANCE.getFile(filePath + ".tmp");
  }

  public void put(Object key, Object value) throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock());
        FileInputStream inputStream = new FileInputStream(formalFile);
        FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
      Properties properties = new Properties();
      properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      properties.put(key, value);
      properties.store(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), "");
      outputStream.getFD().sync();
    }
  }

  public void putAll(Object... keyOrValue) throws IOException {
    if (keyOrValue.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Length of parameters should be evenly divided by 2, but actually they are: "
              + Arrays.toString(keyOrValue));
    }
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock());
        FileInputStream inputStream = new FileInputStream(formalFile);
        FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
      Properties properties = new Properties();
      properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      for (int i = 0; i < keyOrValue.length; i += 2) {
        properties.put(keyOrValue[i], keyOrValue[i + 1]);
      }
      properties.store(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), "");
      outputStream.getFD().sync();
    }
  }

  public void write(Properties properties) throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      if (!formalFile.exists()) {
        writeImpl(properties, formalFile);
        return;
      }
      writeImpl(properties, tmpFile);
      if (!formalFile.delete()) {
        String msg =
            String.format(
                "Update %s file fail: %s", formalFile.getName(), formalFile.getAbsoluteFile());
        throw new IOException(msg);
      }
      FileUtils.moveFileSafe(tmpFile, formalFile);
    }
  }

  public Properties read() throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock());
        FileInputStream inputStream = new FileInputStream(formalFile)) {
      Properties properties = new Properties();
      properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      return properties;
    }
  }

  public boolean fileExist() {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock())) {
      return formalFile.exists() || tmpFile.exists();
    }
  }

  public boolean isRestart() {
    return fileExist();
  }

  private void recover() throws StartupException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      if (formalFile.exists() && !tmpFile.exists()) {
        // No need to recover
        return;
      }
      if (!formalFile.exists() && !tmpFile.exists()) {
        // First start
        return;
      }
      if (formalFile.exists() && tmpFile.exists()) {
        if (!tmpFile.delete()) {
          LOGGER.warn(
              "Delete system.properties tmp file fail, you may manually delete it: {}",
              tmpFile.getAbsoluteFile());
        }
        return;
      }
      if (!formalFile.exists() && tmpFile.exists()) {
        if (!tmpFile.renameTo(formalFile)) {
          String msg =
              String.format(
                  "Cannot rename system properties tmp file, you should manually rename it, then restart the node: %s -> %s",
                  tmpFile.getAbsolutePath(), formalFile.getAbsolutePath());
          throw new StartupException(msg);
        }
        return;
      }
    }
    throw new StartupException("Should never touch here");
  }

  private void writeImpl(Properties properties, File file) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      properties.store(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8), "");
    }
  }

  public static void init(String filePath) throws StartupException {
    Holder.INSTANCE = new SystemPropertiesFileHandler(filePath);
    Holder.INSTANCE.recover();
  }

  public static SystemPropertiesFileHandler getInstance() {
    if (Holder.INSTANCE == null) {
      throw new RuntimeException(
          "You should call SystemPropertiesFileHandler.init before getInstance");
    }
    return Holder.INSTANCE;
  }

  private static class Holder {
    private static SystemPropertiesFileHandler INSTANCE = null;
  }
}
