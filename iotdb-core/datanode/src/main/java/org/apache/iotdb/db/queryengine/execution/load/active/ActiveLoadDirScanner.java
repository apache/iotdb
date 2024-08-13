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

package org.apache.iotdb.db.queryengine.execution.load.active;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ActiveLoadDirScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadDirScanner.class);

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final ScheduledExecutorService DIR_SCAN_JOB_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.ACTIVE_LOAD_DIR_SCANNER.getName());
  private static final long MIN_SCAN_INTERVAL_SECONDS =
      IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningCheckIntervalSeconds();

  private final AtomicReference<String[]> listeningDirsConfig = new AtomicReference<>();
  private final Set<String> listeningDirs = new HashSet<>();

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;

  private Future<?> dirScanJobFuture;

  public ActiveLoadDirScanner(final ActiveLoadTsFileLoader activeLoadTsFileLoader) {
    this.activeLoadTsFileLoader = activeLoadTsFileLoader;
  }

  public synchronized void start() {
    if (dirScanJobFuture == null) {
      dirScanJobFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              DIR_SCAN_JOB_EXECUTOR,
              this::scanSafely,
              MIN_SCAN_INTERVAL_SECONDS,
              MIN_SCAN_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info(
          "Active load dir scanner started. Scan interval: {}s.", MIN_SCAN_INTERVAL_SECONDS);
    }
  }

  public synchronized void stop() {
    if (dirScanJobFuture != null) {
      dirScanJobFuture.cancel(false);
      dirScanJobFuture = null;
      LOGGER.info("Active load dir scanner stopped.");
    }
  }

  private void scanSafely() {
    try {
      scan();
    } catch (final Exception e) {
      LOGGER.warn("Error occurred during active load dir scanning.", e);
    }
  }

  private void scan() throws IOException {
    hotReloadActiveLoadDirs();

    for (final String listeningDir : listeningDirs) {
      final int currentAllowedPendingSize = activeLoadTsFileLoader.getCurrentAllowedPendingSize();
      if (currentAllowedPendingSize <= 0) {
        return;
      }

      final boolean isGeneratedByPipe =
          listeningDir.equals(IOTDB_CONFIG.getLoadActiveListeningPipeDir());
      FileUtils.streamFiles(new File(listeningDir), true, (String[]) null)
          .map(
              file ->
                  (file.getName().endsWith(RESOURCE) || file.getName().endsWith(MODS))
                      ? getTsFilePath(file.getAbsolutePath())
                      : file.getAbsolutePath())
          .filter(this::isTsFileCompleted)
          .limit(currentAllowedPendingSize)
          .forEach(file -> activeLoadTsFileLoader.tryTriggerTsFileLoad(file, isGeneratedByPipe));
    }
  }

  private boolean isTsFileCompleted(final String file) {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file, false)) {
      return TSFileConfig.MAGIC_STRING.equals(reader.readTailMagic());
    } catch (final Exception e) {
      return false;
    }
  }

  private void hotReloadActiveLoadDirs() {
    try {
      // Hot reload active load listening dirs if active listening is enabled
      if (IOTDB_CONFIG.getLoadActiveListeningEnable()) {
        if (IOTDB_CONFIG.getLoadActiveListeningDirs() != listeningDirsConfig.get()) {
          synchronized (this) {
            if (IOTDB_CONFIG.getLoadActiveListeningDirs() != listeningDirsConfig.get()) {
              listeningDirsConfig.set(IOTDB_CONFIG.getLoadActiveListeningDirs());
              listeningDirs.addAll(Arrays.asList(IOTDB_CONFIG.getLoadActiveListeningDirs()));
            }
          }
        }
      }

      // Hot reload active load listening dir for pipe data sync
      // Active load is always enabled for pipe data sync
      listeningDirs.add(IOTDB_CONFIG.getLoadActiveListeningPipeDir());

      // Create directories if not exists
      listeningDirs.forEach(this::createDirectoriesIfNotExists);
    } catch (final Exception e) {
      LOGGER.warn(
          "Error occurred during hot reload active load dirs. "
              + "Current active load listening dirs: {}.",
          listeningDirs,
          e);
    }
  }

  private void createDirectoriesIfNotExists(final String dirPath) {
    try {
      FileUtils.forceMkdir(new File(dirPath));
    } catch (final IOException e) {
      LOGGER.warn("Error occurred during creating directory {} for active load.", dirPath, e);
    }
  }

  private static String getTsFilePath(final String filePathWithResourceOrModsTail) {
    return filePathWithResourceOrModsTail.endsWith(RESOURCE)
        ? filePathWithResourceOrModsTail.substring(
            0, filePathWithResourceOrModsTail.length() - RESOURCE.length())
        : filePathWithResourceOrModsTail.substring(
            0, filePathWithResourceOrModsTail.length() - MODS.length());
  }
}
