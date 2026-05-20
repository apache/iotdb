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

package org.apache.iotdb.confignode.manager.fileloader;

import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class LocalFileLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileLoader.class);

  private static final String CONFIGNODE_HOME_PATH =
      System.getProperty("CONFIGNODE_HOME") == null ? "." : System.getProperty("CONFIGNODE_HOME");
  public static final String ACTIVATION_DIR_PATH =
      CONFIGNODE_HOME_PATH + File.separatorChar + "activation";
  public static final String SYSTEM_INFO_FILE_PATH =
      ACTIVATION_DIR_PATH + File.separatorChar + "system_info";

  private final ReentrantLock loadLock = new ReentrantLock();

  private static final class LocalFileLoaderHolder {

    private static final LocalFileLoader INSTANCE = new LocalFileLoader();

    private LocalFileLoaderHolder() {}
  }

  public static LocalFileLoader getInstance() {
    return LocalFileLoaderHolder.INSTANCE;
  }

  public String loadSystemInfo() throws IOException {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(loadLock);
        FileReader fileReader = new FileReader(SYSTEM_INFO_FILE_PATH)) {
      StringBuilder builder = new StringBuilder();
      int data;
      while ((data = fileReader.read()) != -1) {
        builder.append((char) data);
      }
      return builder.toString();
    } catch (Exception e) {
      LOGGER.error("Load system info file failed", e);
      throw new IOException("Load system info file failed");
    }
  }
}
