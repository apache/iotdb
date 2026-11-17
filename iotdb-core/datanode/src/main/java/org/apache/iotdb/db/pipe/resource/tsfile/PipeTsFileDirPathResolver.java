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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Resolves the pipe directory that holds the hardlinked or copied files of a TsFile. The directory
 * is always on the disk of the TsFile, see {@link PipeTsFileDirPathMapping}.
 *
 * <p>{@code dn_data_dirs} is hot reloadable, so the mapping follows the current local data
 * directories instead of the ones seen at startup.
 */
public class PipeTsFileDirPathResolver {

  private static volatile PipeTsFileDirPathResolver instance;

  private final Supplier<String[]> localDataDirsSupplier;
  private final String pipeHardlinkBaseDirName;
  private final String pipeHardlinkTsFileDirName;

  private volatile String[] resolvedLocalDataDirs;
  private volatile PipeTsFileDirPathMapping mapping;

  private PipeTsFileDirPathResolver(final @Nonnull Supplier<String[]> localDataDirsSupplier) {
    this.localDataDirsSupplier = localDataDirsSupplier;
    this.pipeHardlinkBaseDirName = PipeConfig.getInstance().getPipeHardlinkBaseDirName();
    this.pipeHardlinkTsFileDirName = PipeConfig.getInstance().getPipeHardlinkTsFileDirName();

    this.resolvedLocalDataDirs = localDataDirsSupplier.get();
    this.mapping =
        new PipeTsFileDirPathMapping(
            resolvedLocalDataDirs, pipeHardlinkBaseDirName, pipeHardlinkTsFileDirName);
  }

  @TestOnly
  PipeTsFileDirPathResolver() {
    this(() -> new String[0]);
  }

  public static PipeTsFileDirPathResolver getInstance() {
    if (Objects.isNull(instance)) {
      synchronized (PipeTsFileDirPathResolver.class) {
        if (Objects.isNull(instance)) {
          instance =
              new PipeTsFileDirPathResolver(
                  () -> IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs());
        }
      }
    }
    return instance;
  }

  @TestOnly
  public static void setInstanceForTest(final @Nonnull Supplier<String[]> localDataDirsSupplier) {
    instance = new PipeTsFileDirPathResolver(localDataDirsSupplier);
  }

  @TestOnly
  public static void reset() {
    instance = null;
  }

  /**
   * Returns the hardlink or copied file of {@code file} in the pipe directory of {@code pipeName}.
   */
  static File getHardlinkOrCopiedFileInPipeDir(final File file, final @Nullable String pipeName)
      throws IOException {
    return getInstance().getHardlinkOrCopiedFile(file, pipeName);
  }

  File getHardlinkOrCopiedFile(final File file, final @Nullable String pipeName)
      throws IOException {
    return refresh(localDataDirsSupplier.get()).getHardlinkOrCopiedFile(file, pipeName);
  }

  /** Rebuilds the mapping if the local data directories have been changed by a hot reload. */
  @Nonnull
  PipeTsFileDirPathMapping refresh(final @Nonnull String[] localDataDirs) {
    if (Arrays.equals(localDataDirs, resolvedLocalDataDirs)) {
      return mapping;
    }

    synchronized (this) {
      if (!Arrays.equals(localDataDirs, resolvedLocalDataDirs)) {
        mapping =
            new PipeTsFileDirPathMapping(
                localDataDirs, pipeHardlinkBaseDirName, pipeHardlinkTsFileDirName);
        resolvedLocalDataDirs = localDataDirs;
      }
      return mapping;
    }
  }
}
