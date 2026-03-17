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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

import java.util.Collections;
import java.util.List;

/** The result of decoding an IBF difference. */
public class IBFDecodeResult {

  private final boolean success;
  private final List<DiffEntry> decodedEntries;
  private final int partialCount;

  private IBFDecodeResult(boolean success, List<DiffEntry> decodedEntries, int partialCount) {
    this.success = success;
    this.decodedEntries = decodedEntries;
    this.partialCount = partialCount;
  }

  public static IBFDecodeResult success(List<DiffEntry> entries) {
    return new IBFDecodeResult(true, entries, entries.size());
  }

  public static IBFDecodeResult failure(List<DiffEntry> partialEntries, int partialCount) {
    return new IBFDecodeResult(false, partialEntries, partialCount);
  }

  public boolean isSuccess() {
    return success;
  }

  public List<DiffEntry> getDecodedEntries() {
    return Collections.unmodifiableList(decodedEntries);
  }

  /** Number of entries decoded (may be partial if decode failed). */
  public int getPartialCount() {
    return partialCount;
  }
}
