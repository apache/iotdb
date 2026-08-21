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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import java.util.UUID;

/**
 * One high-cohesion state object per target region of the LOAD consensus two-phase protocol. It
 * replaces the five parallel maps ({@code regionPieceCount}, {@code regionPieceTotalBytes}, {@code
 * regionPieceChecksum}, {@code begunConsensusRegions}, {@code regionLoadId}) that used to live in
 * {@link LoadTsFileScheduler}, so the load id, the BEGIN state and the three accumulated counters
 * can never diverge.
 *
 * <p>Lifecycle, driven by {@link TwoPhaseConsensusLoadStrategy}:
 *
 * <ul>
 *   <li>Created lazily when the first piece of a region is dispatched ({@code
 *       consensusContexts.computeIfAbsent}).
 *   <li>{@link #markBegun()} runs before the BEGIN command is submitted, so a region is begun at
 *       most once per load.
 *   <li>{@link #accumulate(long, long)} runs after every successful PIECE: it increments the piece
 *       count, adds the piece bytes and XORs the piece checksum.
 *   <li>Phase two reads {@link #getPieceCount()}, {@link #getTotalBytes()} and {@link
 *       #getChecksum()} to build the PREPARE command.
 * </ul>
 *
 * The load id is generated per instance, so every region of the same source file is isolated from
 * the others on the write nodes.
 */
public class RegionConsensusContext {

  /** Each region gets its own load id so its staged data is isolated from other regions. */
  private final String loadId = UUID.randomUUID().toString();

  private long pieceCount = 0;
  private long totalBytes = 0;
  private long checksum = 0;

  /** Whether the BEGIN command has already been sent to this region. */
  private boolean begun = false;

  public String getLoadId() {
    return loadId;
  }

  public long getPieceCount() {
    return pieceCount;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public long getChecksum() {
    return checksum;
  }

  public boolean isBegun() {
    return begun;
  }

  public void markBegun() {
    begun = true;
  }

  /** Records one successfully applied piece: increments the count, adds bytes and XORs checksum. */
  public void accumulate(long bytes, long pieceChecksum) {
    pieceCount++;
    totalBytes += bytes;
    checksum ^= pieceChecksum;
  }
}
