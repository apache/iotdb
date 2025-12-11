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

package org.apache.iotdb.db.pipe.sink.util.builder;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PipeTsFileIdGenerator - Global TSFile ID Generator
 *
 * <p>Provides unified BatchID and TSFile ID allocation, as well as file name generation rules
 */
public class PipeTsFileIdGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileIdGenerator.class);

  // Global BatchID generator
  private static final AtomicLong GLOBAL_BATCH_ID_GENERATOR = new AtomicLong(0);

  // Global TSFile ID generator
  private static final AtomicLong GLOBAL_TSFILE_ID_GENERATOR = new AtomicLong(0);

  // TSFile file name prefix
  private static final String TS_FILE_PREFIX = "tb"; // tb means tablet batch

  /**
   * Get the next BatchID
   *
   * @return the next BatchID
   */
  public static long getNextBatchId() {
    return GLOBAL_BATCH_ID_GENERATOR.incrementAndGet();
  }

  /**
   * Get the next TSFile ID
   *
   * @return the next TSFile ID
   */
  public static long getNextTsFileId() {
    return GLOBAL_TSFILE_ID_GENERATOR.incrementAndGet();
  }

  /**
   * Generate temporary TSFile file name
   *
   * <p>File name format: tb_{dataNodeId}_{batchId}_{tsFileId}.tsfile
   *
   * @param dataNodeId DataNode ID
   * @param batchId Batch ID
   * @param tsFileId TSFile ID
   * @return temporary TSFile file name
   */
  public static String generateTempTsFileName(
      final int dataNodeId, final long batchId, final long tsFileId) {
    return TS_FILE_PREFIX
        + "_"
        + dataNodeId
        + "_"
        + batchId
        + "_"
        + tsFileId
        + TsFileConstant.TSFILE_SUFFIX;
  }

  /**
   * Generate temporary TSFile file path
   *
   * @param baseDir base directory
   * @param dataNodeId DataNode ID
   * @param batchId Batch ID
   * @param tsFileId TSFile ID
   * @return temporary TSFile file path
   */
  public static File generateTempTsFilePath(
      final File baseDir, final int dataNodeId, final long batchId, final long tsFileId) {
    return new File(baseDir, generateTempTsFileName(dataNodeId, batchId, tsFileId));
  }

  /**
   * Parse BatchID from temporary file name
   *
   * @param fileName temporary file name (format: tb_{dataNodeId}_{batchId}_{tsFileId}.tsfile)
   * @return BatchID, or -1 if parsing fails
   */
  public static long parseBatchIdFromTempFileName(final String fileName) {
    try {
      if (fileName == null || !fileName.startsWith(TS_FILE_PREFIX + "_")) {
        return -1;
      }
      final String[] parts = fileName.substring(TS_FILE_PREFIX.length() + 1).split("_");
      if (parts.length >= 2) {
        return Long.parseLong(parts[1]);
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to parse batch ID from file name: {}", fileName, e);
    }
    return -1;
  }

  /**
   * Parse TSFile ID from temporary file name
   *
   * @param fileName temporary file name (format: tb_{dataNodeId}_{batchId}_{tsFileId}.tsfile)
   * @return TSFile ID, or -1 if parsing fails
   */
  public static long parseTsFileIdFromTempFileName(final String fileName) {
    try {
      if (fileName == null || !fileName.startsWith(TS_FILE_PREFIX + "_")) {
        return -1;
      }
      final String[] parts = fileName.substring(TS_FILE_PREFIX.length() + 1).split("_");
      if (parts.length >= 3) {
        final String tsFileIdPart = parts[2];
        // Remove .tsfile suffix
        final int suffixIndex = tsFileIdPart.indexOf(TsFileConstant.TSFILE_SUFFIX);
        if (suffixIndex > 0) {
          return Long.parseLong(tsFileIdPart.substring(0, suffixIndex));
        }
        return Long.parseLong(tsFileIdPart);
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to parse TSFile ID from file name: {}", fileName, e);
    }
    return -1;
  }

  /**
   * Generate final TSFile file name (for renaming)
   *
   * <p>Can customize naming rules as needed, for example: - Keep original file name - Use
   * timestamp: {timestamp}.tsfile - Use sequence number: tsfile_{sequence}.tsfile
   *
   * @param originalFileName original temporary file name
   * @param targetDirectory target directory
   * @param sequenceNumber sequence number (optional, for generating unique file names)
   * @return final TSFile file name
   */
  public static String generateFinalTsFileName(
      final String originalFileName, final File targetDirectory, final long sequenceNumber) {
    // Default to keep original file name, add sequence number if file already exists in target
    // directory
    final String baseName = originalFileName != null ? originalFileName : "tsfile";
    final File targetFile = new File(targetDirectory, baseName);

    if (targetFile.exists() && sequenceNumber > 0) {
      // If file exists, add sequence number
      final String nameWithoutSuffix = baseName.replace(TsFileConstant.TSFILE_SUFFIX, "");
      return nameWithoutSuffix + "_" + sequenceNumber + TsFileConstant.TSFILE_SUFFIX;
    }

    return baseName;
  }

  /**
   * Validate if file name conforms to temporary file naming rules
   *
   * @param fileName file name
   * @return true if conforms to rules, false otherwise
   */
  public static boolean isValidTempFileName(final String fileName) {
    if (fileName == null || !fileName.endsWith(TsFileConstant.TSFILE_SUFFIX)) {
      return false;
    }
    return fileName.startsWith(TS_FILE_PREFIX + "_");
  }
}
