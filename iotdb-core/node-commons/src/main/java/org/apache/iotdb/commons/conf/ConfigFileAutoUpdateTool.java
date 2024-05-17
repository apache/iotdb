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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class ConfigFileAutoUpdateTool {

  private final String lockFileSuffix = ".lock";
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

    boolean canUpdate = configNodeFile.exists() && dataNodeFile.exists() && commonFile.exists();
    if (!canUpdate) {
      return;
    }

    if (!systemFile.exists()) {
      systemFile.createNewFile();
    }

    acquireTargetFileLock(systemFile);
    try {
      // other progress updated this file
      if (systemFile.length() != 0) {
        return;
      }
      try (RandomAccessFile raf = new RandomAccessFile(systemFile, "rw")) {
        raf.write(license.getBytes());
        String configNodeContent = readConfigLines(configNodeFile);
        raf.write(configNodeContent.getBytes());
        String dataNodeContent = readConfigLines(dataNodeFile);
        raf.write(dataNodeContent.getBytes());
        String commonContent = readConfigLines(commonFile);
        raf.write(commonContent.getBytes());
      }
    } finally {
      releaseFileLock(systemFile);
    }
  }

  private String readConfigLines(File file) throws IOException {
    byte[] bytes = Files.readAllBytes(file.toPath());
    String content = new String(bytes);
    return content.replace(license, "");
  }

  private void acquireTargetFileLock(File file) throws IOException, InterruptedException {
    long waitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
    File lockFile = new File(file.getPath() + lockFileSuffix);
    while (System.currentTimeMillis() < waitTime) {
      if (lockFile.createNewFile()) {
        return;
      }
      Thread.sleep(TimeUnit.MICROSECONDS.toMillis(100));
    }
  }

  private void releaseFileLock(File file) throws IOException {
    Files.deleteIfExists(new File(file.getPath() + lockFileSuffix).toPath());
  }
}
