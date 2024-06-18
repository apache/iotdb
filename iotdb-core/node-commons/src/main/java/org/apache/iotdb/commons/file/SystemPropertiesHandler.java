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

import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SystemPropertiesHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesHandler.class);

  private final File formalFile;
  private final File tmpFile;
  //    private ImmutableProperties cache;
  ReadWriteLock lock = new ReentrantReadWriteLock();

  private SystemPropertiesHandler(String filePath) {
    this.formalFile = SystemFileFactory.INSTANCE.getFile(filePath);
    this.tmpFile = SystemFileFactory.INSTANCE.getFile(filePath + ".tmp");
  }

  /**
   * Put key and value into properties
   *
   * @param keyOrValue [key0, value0, key1, value1, ... ]
   */
  public void put(Object... keyOrValue) throws IOException {
    if (keyOrValue.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Length of parameters should be evenly divided by 2, but the actual length is "
              + keyOrValue.length
              + " : "
              + Arrays.toString(keyOrValue));
    }
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock());
        FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
      Properties properties = new Properties();
      // read old properties
      if (formalFile.exists()) {
        try (FileInputStream inputStream = new FileInputStream(formalFile)) {
          properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        }
      }
      // store new properties
      for (int i = 0; i < keyOrValue.length; i += 2) {
        properties.put(keyOrValue[i], keyOrValue[i + 1]);
      }
      properties.store(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), "");
      outputStream.getFD().sync();
      replaceFormalFile();
    }
  }

  public void overwrite(Properties properties) throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      if (!formalFile.exists()) {
        writeImpl(properties, formalFile);
        return;
      }
      writeImpl(properties, tmpFile);
      replaceFormalFile();
    }
  }

  public Properties read() throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock())) {
      Properties properties = new Properties();
      if (formalFile.exists()) {
        try (FileInputStream inputStream = new FileInputStream(formalFile)) {
          properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        }
      }
      return properties;
    }
  }

  public boolean fileExist() {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock())) {
      return formalFile.exists() || tmpFile.exists();
    }
  }

  public boolean isFirstStart() {
    return !fileExist();
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
        replaceFormalFile();
        return;
      }
    } catch (IOException e) {
      throw new StartupException(e);
    }
    throw new StartupException("Should never touch here");
  }

  private void writeImpl(Properties properties, File file) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      properties.store(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8), "");
    }
  }

  private void replaceFormalFile() throws IOException {
    if (!tmpFile.exists()) {
      throw new FileNotFoundException(
          "Tmp system properties file must exist when call replaceFormalFile");
    }
    if (formalFile.exists() && !formalFile.delete()) {
      String msg =
          String.format(
              "Delete formal system properties file fail: %s", formalFile.getAbsoluteFile());
      throw new IOException(msg);
    }
    if (!tmpFile.renameTo(formalFile)) {
      String msg =
          String.format(
              "Failed to replace formal system properties file, you may manually rename it: %s -> %s",
              tmpFile.getAbsolutePath(), formalFile.getAbsolutePath());
      throw new IOException(msg);
    }
  }

  public static void init(String filePath) throws StartupException {
    Holder.INSTANCE = new SystemPropertiesHandler(filePath);
    Holder.INSTANCE.recover();
  }

  public static SystemPropertiesHandler getInstance() {
    if (Holder.INSTANCE == null) {
      LOGGER.warn(
          "The SystemPropertiesHandler's instance has not been inited yet, call getInstance may cause NullPointerException");
    }
    return Holder.INSTANCE;
  }

  private static class Holder {
    private static SystemPropertiesHandler INSTANCE = null;
  }
}
