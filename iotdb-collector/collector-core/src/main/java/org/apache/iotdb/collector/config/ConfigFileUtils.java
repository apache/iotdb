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

package org.apache.iotdb.collector.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class ConfigFileUtils {

  private static final String lockFileSuffix = ".lock";
  private static final long maxTimeMillsToAcquireLock = TimeUnit.SECONDS.toMillis(20);
  private static final long waitTimeMillsPerCheck = TimeUnit.MILLISECONDS.toMillis(100);
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigFileUtils.class);
  private static final String lineSeparator = "\n";
  private static final String license =
      new StringJoiner(lineSeparator)
          .add("# Licensed to the Apache Software Foundation (ASF) under one")
          .add("# or more contributor license agreements.  See the NOTICE file")
          .add("# distributed with this work for additional information")
          .add("# regarding copyright ownership.  The ASF licenses this file")
          .add("# to you under the Apache License, Version 2.0 (the")
          .add("# \"License\"); you may not use this file except in compliance")
          .add("# with the License.  You may obtain a copy of the License at")
          .add("#")
          .add("#     http://www.apache.org/licenses/LICENSE-2.0")
          .add("#")
          .add("# Unless required by applicable law or agreed to in writing,")
          .add("# software distributed under the License is distributed on an")
          .add("# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY")
          .add("# KIND, either express or implied.  See the License for the")
          .add("# specific language governing permissions and limitations")
          .add("# under the License.")
          .toString();

  public static void checkAndMayUpdate(final URL url) throws IOException, InterruptedException {
    final File systemFile = new File(url.getFile());
    if (systemFile.exists()) {
      return;
    }
    final File lockFile = new File(systemFile.getAbsolutePath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      if (systemFile.exists()) {
        return;
      }
      try (final RandomAccessFile raf = new RandomAccessFile(lockFile, "rw")) {
        raf.write(license.getBytes());
      }
      Files.move(lockFile.toPath(), systemFile.toPath());
    } finally {
      releaseFileLock(lockFile);
    }
  }

  private static void acquireTargetFileLock(final File file)
      throws IOException, InterruptedException {
    long totalWaitTime = 0;
    while (totalWaitTime < maxTimeMillsToAcquireLock) {
      if (file.createNewFile()) {
        return;
      }
      totalWaitTime += waitTimeMillsPerCheck;
      Thread.sleep(waitTimeMillsPerCheck);
    }
    LOGGER.warn(
        "Waiting for {} seconds to acquire configuration file update lock."
            + " There may have been an unexpected interruption in the last"
            + " configuration file update. Ignore temporary file {}",
        totalWaitTime / 1000,
        file.getName());
  }

  private static void releaseFileLock(final File file) throws IOException {
    Files.deleteIfExists(file.toPath());
  }
}
