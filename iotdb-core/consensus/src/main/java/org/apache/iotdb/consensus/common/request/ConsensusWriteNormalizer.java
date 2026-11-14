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

package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The same normalization and completion rule for Raft, ParallelRaft and paper TRaft. */
public final class ConsensusWriteNormalizer {
  @FunctionalInterface
  public interface SingleWriter {
    TSStatus write(IConsensusRequest request) throws ConsensusException;
  }

  @FunctionalInterface
  public interface AsyncWriter {
    CompletableFuture<TSStatus> submit(IConsensusRequest request) throws ConsensusException;
  }

  @FunctionalInterface
  public interface ResultAwaiter {
    TSStatus await(CompletableFuture<TSStatus> pending) throws ConsensusException;
  }

  private ConsensusWriteNormalizer() {}

  public static NormalizedRequest normalize(IConsensusRequest request) {
    return request instanceof RequestNormalizationProvider
        ? ((RequestNormalizationProvider) request).normalizeRequest()
        : NormalizedRequest.single(request);
  }

  /**
   * Admits children in received order and maps each returned result. A writer may return a
   * pre-admission row rejection and continue with later children; this helper does not establish
   * that a failed state-machine application was deterministic. Research backends fence on any
   * non-successful application result and stop admission. A prefix may already have applied, so
   * callers must not interpret a retry as an atomic transaction or exactly once.
   */
  public static TSStatus write(IConsensusRequest request, SingleWriter writer)
      throws ConsensusException {
    NormalizedRequest normalized = normalize(request);
    TSStatus failure = null;
    TSStatus lastSuccess = null;
    for (int i = 0; i < normalized.size(); i++) {
      TSStatus result = writer.write(normalized.requestAt(i));
      if (result == null) {
        throw new ConsensusException("No application result for normalized row " + i);
      }
      normalized.recordResult(i, result);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        lastSuccess = result;
      } else {
        if (!isMappableRowRejection(result)) {
          return result;
        }
        if (failure == null
            || normalized.failureSelection() == NormalizedRequest.FailureSelection.LAST) {
          failure = result;
        }
      }
    }
    return failure == null ? lastSuccess : failure;
  }

  /**
   * As {@link #write}, but submits every child before waiting for the first result, so no child
   * waits for an earlier one's commit. Results are mapped and selected exactly as in write, in
   * received order. A failure can no longer stop the admission of later children: when an error
   * returns, any subset of the children, not only a prefix, may have applied.
   */
  public static TSStatus writePipelined(
      IConsensusRequest request, AsyncWriter writer, ResultAwaiter awaiter)
      throws ConsensusException {
    NormalizedRequest normalized = normalize(request);
    List<CompletableFuture<TSStatus>> pending = new ArrayList<>(normalized.size());
    for (int i = 0; i < normalized.size(); i++) {
      pending.add(writer.submit(normalized.requestAt(i)));
    }
    TSStatus failure = null;
    TSStatus lastSuccess = null;
    for (int i = 0; i < normalized.size(); i++) {
      TSStatus result = awaiter.await(pending.get(i));
      if (result == null) {
        throw new ConsensusException("No application result for normalized row " + i);
      }
      normalized.recordResult(i, result);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        lastSuccess = result;
      } else {
        if (!isMappableRowRejection(result)) {
          return result;
        }
        if (failure == null
            || normalized.failureSelection() == NormalizedRequest.FailureSelection.LAST) {
          failure = result;
        }
      }
    }
    return failure == null ? lastSuccess : failure;
  }

  private static boolean isMappableRowRejection(TSStatus status) {
    // This list governs returned pre-admission results only, never applied-frontier advancement.
    // TTL and metadata errors are not guaranteed equal across replica-local application attempts.
    // Unknown or nested statuses deliberately stop the rest of a batch.
    if (status.isSetSubStatus() && !status.getSubStatus().isEmpty()) {
      return false;
    }
    int code = status.getCode();
    return code == TSStatusCode.OUT_OF_TTL.getStatusCode()
        || code == TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode()
        || code == TSStatusCode.ALIGNED_TIMESERIES_ERROR.getStatusCode()
        || code == TSStatusCode.PATH_NOT_EXIST.getStatusCode()
        || code == TSStatusCode.ILLEGAL_PATH.getStatusCode();
  }
}
