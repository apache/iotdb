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

import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.util.UUID;
import java.util.regex.Pattern;

public class TsFileNameGenerator {

  private static final Pattern NON_FILE_NAME_CHAR_PATTERN = Pattern.compile("[^A-Za-z0-9._-]");

  private final String runId;
  private long lastTimestamp;
  private long indexSeq;

  public TsFileNameGenerator() {
    this.runId = UUID.randomUUID().toString();
    this.lastTimestamp = System.currentTimeMillis();
    this.indexSeq = 0;
  }

  public String nextFileName() {
    long now = System.currentTimeMillis();

    if (now > lastTimestamp) {
      lastTimestamp = now;
      indexSeq = 0;
    }

    long currentId = indexSeq++;

    return String.format("%s_%d_%d", runId, lastTimestamp, currentId);
  }

  public String getRunId() {
    return runId;
  }

  public static String targetNameForEvent(final TsFileInsertionEvent event) {
    if (event == null) {
      return "tsfile";
    }

    if (event instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
      final CommitterKey committerKey = enrichedEvent.getCommitterKey();
      if (committerKey != null && enrichedEvent.getCommitId() != EnrichedEvent.NO_COMMIT_ID) {
        return sanitizeFileName(
            stripTsFileSuffix(event.getTsFile())
                + "_"
                + committerKey.stringify()
                + "_"
                + enrichedEvent.getCommitId());
      }
    }

    return targetNameForFile(event.getTsFile());
  }

  public static String targetNameForFile(final File tsFile) {
    if (tsFile == null) {
      return "tsfile";
    }

    final String normalizedPath = tsFile.toPath().toAbsolutePath().normalize().toString();
    return sanitizeFileName(
        stripTsFileSuffix(tsFile) + "_" + Integer.toUnsignedString(normalizedPath.hashCode()));
  }

  public static String targetNameForGeneratedFile(final File tsFile) {
    return sanitizeFileName(stripTsFileSuffix(tsFile));
  }

  private static String stripTsFileSuffix(final File tsFile) {
    return stripTsFileSuffix(tsFile == null ? null : tsFile.getName());
  }

  private static String stripTsFileSuffix(final String tsFileName) {
    if (tsFileName == null || tsFileName.isEmpty()) {
      return "tsfile";
    }
    return tsFileName.endsWith(TsFileConstant.TSFILE_SUFFIX)
        ? tsFileName.substring(0, tsFileName.length() - TsFileConstant.TSFILE_SUFFIX.length())
        : tsFileName;
  }

  private static String sanitizeFileName(final String rawName) {
    final String sanitized = NON_FILE_NAME_CHAR_PATTERN.matcher(rawName).replaceAll("_");
    return sanitized.isEmpty() ? "tsfile" : sanitized;
  }
}
