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

package org.apache.iotdb.commons.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class ConfigFileAutoUpdateTool {

  private final String lockFileSuffix = ".lock";
  private final long maxTimeMillsToAcquireLock = TimeUnit.SECONDS.toMillis(20);
  private final long waitTimeMillsPerCheck = TimeUnit.MILLISECONDS.toMillis(100);
  private Logger logger = LoggerFactory.getLogger(ConfigFileAutoUpdateTool.class);
  private String license =
      "#\n"
          + "# Licensed to the Apache Software Foundation (ASF) under one\n"
          + "# or more contributor license agreements.  See the NOTICE file\n"
          + "# distributed with this work for additional information\n"
          + "# regarding copyright ownership.  The ASF licenses this file\n"
          + "# to you under the Apache License, Version 2.0 (the\n"
          + "# \"License\"); you may not use this file except in compliance\n"
          + "# with the License.  You may obtain a copy of the License at\n"
          + "#\n"
          + "#     http://www.apache.org/licenses/LICENSE-2.0\n"
          + "#\n"
          + "# Unless required by applicable law or agreed to in writing,\n"
          + "# software distributed under the License is distributed on an\n"
          + "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
          + "# KIND, either express or implied.  See the License for the\n"
          + "# specific language governing permissions and limitations\n"
          + "# under the License.";

  public void checkAndMayUpdate(URL systemUrl, URL configNodeUrl, URL dataNodeUrl, URL commonUrl)
      throws IOException, InterruptedException {
    if (systemUrl == null || configNodeUrl == null || dataNodeUrl == null || commonUrl == null) {
      return;
    }
    File systemFile = new File(systemUrl.getFile());
    File configNodeFile = new File(configNodeUrl.getFile());
    File dataNodeFile = new File(dataNodeUrl.getFile());
    File commonFile = new File(commonUrl.getFile());

    if (systemFile.exists()) {
      return;
    }
    boolean canUpdate = (configNodeFile.exists() || dataNodeFile.exists()) && commonFile.exists();
    if (!canUpdate) {
      return;
    }

    File lockFile = new File(systemFile.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      // other progress updated this file
      if (systemFile.exists()) {
        return;
      }
      try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw")) {
        raf.write(license.getBytes());
        String configNodeContent = readConfigLines(configNodeFile);
        raf.write(configNodeContent.getBytes());
        String dataNodeContent = readConfigLines(dataNodeFile);
        raf.write(dataNodeContent.getBytes());
        String commonContent = readConfigLines(commonFile);
        raf.write(commonContent.getBytes());
      }
      Files.move(lockFile.toPath(), systemFile.toPath());
    } finally {
      releaseFileLock(lockFile);
    }
  }

  private String readConfigLines(File file) throws IOException {
    if (!file.exists()) {
      return "";
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    String content = new String(bytes);
    return content.replace(license, "");
  }

  private void acquireTargetFileLock(File file) throws IOException, InterruptedException {
    long totalWaitTime = 0;
    while (totalWaitTime < maxTimeMillsToAcquireLock) {
      if (file.createNewFile()) {
        return;
      }
      totalWaitTime += waitTimeMillsPerCheck;
      Thread.sleep(waitTimeMillsPerCheck);
    }
    logger.warn(
        "Waiting for {} seconds to acquire configuration file update lock."
            + " There may have been an unexpected interruption in the last"
            + " configuration file update. Ignore temporary file {}",
        totalWaitTime / 1000,
        file.getName());
  }

  private void releaseFileLock(File file) throws IOException {
    Files.deleteIfExists(file.toPath());
  }
}
