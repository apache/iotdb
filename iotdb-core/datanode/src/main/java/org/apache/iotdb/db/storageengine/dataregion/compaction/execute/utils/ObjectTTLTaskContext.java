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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Per-table error context for a single Object file TTL scan.
 *
 * <p>Created once per table directory in {@code executeTTLCheckObjectFilesForTableModel}, carried
 * through the {@code recursiveTTLCheckForTableDir} recursion, and read back at the end to produce a
 * failure audit log when unexpected errors occurred.
 *
 * <p>Callers decide at each catch site whether an exception is expected (e.g. {@code
 * StopTTLCheckException}, {@code InterruptedException}, concurrent-deletion exceptions,
 * filename-parse failures) and only call {@link #recordError(Exception)} for truly unexpected
 * errors.
 */
public class ObjectTTLTaskContext {

  private final boolean auditEnabled;
  private Map<String, ExceptionErrorInfo> errorInfos;

  public ObjectTTLTaskContext() {
    this.auditEnabled = CompactionUtils.isFailedTTLDeletionAuditEnabled();
  }

  /**
   * Record an unexpected exception. Only called from catch sites that have already determined the
   * exception is not part of normal control flow.
   *
   * <p>Exceptions are grouped by class name. For each type the message of the first occurrence is
   * sampled; subsequent ones only increment the count.
   */
  public void recordError(Exception e) {
    if (!auditEnabled) {
      return;
    }
    if (errorInfos == null) {
      errorInfos = new LinkedHashMap<>();
    }
    errorInfos
        .computeIfAbsent(e.getClass().getSimpleName(), k -> new ExceptionErrorInfo(e.getMessage()))
        .add();
  }

  /** Returns an unmodifiable view of the per-exception-type metadata, if any was recorded. */
  public Map<String, ExceptionErrorInfo> getErrorInfos() {
    if (errorInfos == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(errorInfos);
  }

  /**
   * Returns a compact summary string suitable for the audit-log field, e.g. {@code "[IOException
   * x3(Permission denied),SecurityException x1(Access denied)]"}. Returns {@code "[]"} when there
   * are no errors.
   */
  public String getErrorSummary() {
    if (errorInfos == null || errorInfos.isEmpty()) {
      return "[]";
    }
    return "["
        + errorInfos.entrySet().stream()
            .map(
                e ->
                    e.getKey()
                        + " x"
                        + e.getValue().getCount()
                        + (e.getValue().getSampleMessage() != null
                            ? "(" + e.getValue().getSampleMessage() + ")"
                            : ""))
            .collect(Collectors.joining(","))
        + "]";
  }

  /** Per-type error metadata: a count and the message of the first sampled exception. */
  public static final class ExceptionErrorInfo {
    private final String sampleMessage;
    private int count;

    ExceptionErrorInfo(String sampleMessage) {
      this.sampleMessage = sampleMessage;
      this.count = 0;
    }

    void add() {
      count++;
    }

    public int getCount() {
      return count;
    }

    public String getSampleMessage() {
      return sampleMessage;
    }
  }
}
