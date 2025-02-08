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

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class SystemPropertiesHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesHandler.class);

  private File formalFile;
  private File tmpFile;
  ReadWriteLock lock = new ReentrantReadWriteLock();

  public SystemPropertiesHandler(String filePath) {
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
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      // read old properties
      Properties properties = readWithoutLock(formalFile);
      // store new properties
      for (int i = 0; i < keyOrValue.length; i += 2) {
        properties.put(keyOrValue[i], keyOrValue[i + 1]);
      }
      writeWithoutLock(properties, tmpFile);
      // replace file
      replaceFormalFile();
    }
  }

  public void overwrite(Properties properties) throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      if (!formalFile.exists()) {
        writeWithoutLock(properties, formalFile);
        return;
      }
      writeWithoutLock(properties, tmpFile);
      replaceFormalFile();
    }
  }

  public Properties read() throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock())) {
      return readWithoutLock(formalFile);
    }
  }

  public void remove(String key) throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      Properties properties = readWithoutLock(formalFile);
      if (!properties.containsKey(key)) {
        return;
      }
      properties.remove(key);
      writeWithoutLock(properties, tmpFile);
      replaceFormalFile();
    }
  }

  private void writeWithoutLock(Properties properties, File file) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
        Writer writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
      properties.store(writer, "");
      fileOutputStream.getFD().sync();
    }
  }

  private Properties readWithoutLock(File file) throws IOException {
    Properties properties = new Properties();
    if (file.exists()) {
      try (FileInputStream inputStream = new FileInputStream(formalFile);
          Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        properties.load(reader);
      }
    }
    return properties;
  }

  public boolean fileExist() {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.readLock())) {
      return formalFile.exists() || tmpFile.exists();
    }
  }

  public boolean isFirstStart() {
    return !fileExist();
  }

  protected void init() {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(lock.writeLock())) {
      recover();
      // TODO: this line should be removed in 1.5
      remove(IoTDBConstant.CLUSTER_NAME);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void recover() throws IOException {
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
    throw new UnsupportedOperationException("Should never touch here");
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
    try {
      FileUtils.move(tmpFile.toPath(), formalFile.toPath());
    } catch (IOException e) {
      String msg =
          String.format(
              "Failed to replace formal system properties file, you may manually rename it: %s -> %s",
              tmpFile.getAbsolutePath(), formalFile.getAbsolutePath());
      throw new IOException(msg, e);
    }
  }

  public void resetFilePath(String filePath) {
    this.formalFile = SystemFileFactory.INSTANCE.getFile(filePath);
    this.tmpFile = SystemFileFactory.INSTANCE.getFile(filePath + ".tmp");
  }

  public void delete() {
    this.formalFile.delete();
    this.tmpFile.delete();
    if (this.formalFile.exists() || this.tmpFile.exists()) {
      LOGGER.warn(
          "Failed to delete system.properties file, you should manually delete them: {}, {}",
          this.formalFile.getAbsoluteFile(),
          this.tmpFile.getAbsolutePath());
    }
  }
}
