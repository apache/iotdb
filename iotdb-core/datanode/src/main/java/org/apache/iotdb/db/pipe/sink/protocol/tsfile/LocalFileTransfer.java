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

package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * LocalFileTransfer - Local file transfer implementation
 *
 * <p>Use local file system for file copying
 */
public class LocalFileTransfer implements FileTransfer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileTransfer.class);

  private final String targetPath;

  public LocalFileTransfer(final String targetPath) {
    this.targetPath = targetPath;
  }

  @Override
  public void testConnection() throws Exception {
    // Local mode does not need to test connection, just check if target directory exists
    final File targetDir = new File(targetPath);
    if (!targetDir.exists()) {
      if (!targetDir.mkdirs()) {
        throw new PipeException("Failed to create target directory: " + targetPath);
      }
      LOGGER.info("Created target directory: {}", targetPath);
    } else if (!targetDir.isDirectory()) {
      throw new PipeException("Target path is not a directory: " + targetPath);
    }
    LOGGER.debug("Local file transfer target directory verified: {}", targetPath);
  }

  @Override
  public void transferFile(final File localFile) throws Exception {
    if (!localFile.exists()) {
      throw new PipeException("Local file does not exist: " + localFile);
    }

    if (localFile.isDirectory()) {
      transferDirectory(localFile);
      return;
    }

    // Build target file path
    final File targetFile = new File(targetPath, localFile.getName());

    // Ensure target directory exists
    final File targetDir = targetFile.getParentFile();
    if (targetDir != null && !targetDir.exists()) {
      if (!targetDir.mkdirs()) {
        throw new PipeException("Failed to create target directory: " + targetDir);
      }
    }

    // Copy file
    try {
      Files.copy(localFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      LOGGER.info("Copied file {} to {}", localFile, targetFile);
    } catch (final IOException e) {
      throw new PipeException(
          String.format("Failed to copy file %s to %s: %s", localFile, targetFile, e.getMessage()),
          e);
    }
  }

  @Override
  public void transferDirectory(final File localDir) throws Exception {
    if (!localDir.exists() || !localDir.isDirectory()) {
      throw new PipeException("Local directory does not exist or is not a directory: " + localDir);
    }

    // Build target directory path
    final File targetDir = new File(targetPath, localDir.getName());

    // Ensure target parent directory exists
    final File targetParentDir = targetDir.getParentFile();
    if (targetParentDir != null && !targetParentDir.exists()) {
      if (!targetParentDir.mkdirs()) {
        throw new PipeException("Failed to create target parent directory: " + targetParentDir);
      }
    }

    // Recursively copy directory
    try {
      copyDirectoryRecursive(localDir, targetDir);
      LOGGER.info("Copied directory {} to {}", localDir, targetDir);
    } catch (final IOException e) {
      throw new PipeException(
          String.format(
              "Failed to copy directory %s to %s: %s", localDir, targetDir, e.getMessage()),
          e);
    }
  }

  /**
   * Recursively copy directory
   *
   * @param sourceDir source directory
   * @param targetDir target directory
   * @throws IOException if copy fails
   */
  private void copyDirectoryRecursive(final File sourceDir, final File targetDir)
      throws IOException {
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      throw new IOException("Failed to create target directory: " + targetDir);
    }

    final File[] files = sourceDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        final File targetFile = new File(targetDir, file.getName());
        if (file.isDirectory()) {
          copyDirectoryRecursive(file, targetFile);
        } else {
          Files.copy(file.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    // Local mode does not need to close connection
    LOGGER.debug("LocalFileTransfer closed");
  }
}
