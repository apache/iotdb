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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import java.util.List;

public abstract class SearchNode extends WritePlanNode implements ComparableConsensusRequest {

  private static final long LAST_FRAGMENT_MASK = Long.MIN_VALUE;

  /** this insert node doesn't need to participate in iot consensus */
  public static final long NO_CONSENSUS_INDEX = ConsensusReqReader.DEFAULT_SEARCH_INDEX;

  // Preserve last-fragment state for WAL entries that do not have a consensus search index.
  private static final long NO_CONSENSUS_INDEX_WITH_LAST_FRAGMENT = Long.MIN_VALUE;

  /**
   * this index is used by wal search, its order should be protected by the upper layer, and the
   * value should start from 1
   */
  protected long searchIndex = NO_CONSENSUS_INDEX;

  /** routing epoch from ConfigNode broadcast, used for ordered consensus subscription */
  protected long routingEpoch = 0;

  /** Millisecond physical time used as the first ordering key in the new subscription progress. */
  protected long physicalTime = 0;

  /** Writer node id used as the second ordering key across multiple writers. */
  protected int nodeId = -1;

  /**
   * syncIndex carries the source Leader's searchIndex for replicated (Follower) writes. On Leader
   * nodes this stays at NO_CONSENSUS_INDEX (-1). Only stored in WALMetaData V3, never changes the
   * WAL entry's own searchIndex.
   */
  protected long syncIndex = NO_CONSENSUS_INDEX;

  protected boolean isLastFragment = false;

  protected SearchNode(PlanNodeId id) {
    super(id);
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  /** Search index should start from 1 */
  public SearchNode setSearchIndex(long searchIndex) {
    this.searchIndex = searchIndex;
    return this;
  }

  public long getRoutingEpoch() {
    return routingEpoch;
  }

  public SearchNode setRoutingEpoch(long routingEpoch) {
    this.routingEpoch = routingEpoch;
    return this;
  }

  public long getPhysicalTime() {
    return physicalTime;
  }

  public SearchNode setPhysicalTime(long physicalTime) {
    this.physicalTime = physicalTime;
    return this;
  }

  public int getNodeId() {
    return nodeId;
  }

  public SearchNode setNodeId(int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public long getSyncIndex() {
    return syncIndex;
  }

  public SearchNode setSyncIndex(long syncIndex) {
    this.syncIndex = syncIndex;
    return this;
  }

  public long getLocalSeq() {
    return searchIndex;
  }

  public SearchNode setLocalSeq(long localSeq) {
    this.searchIndex = localSeq;
    return this;
  }

  public long getProgressLocalSeq() {
    return syncIndex >= 0 ? syncIndex : searchIndex;
  }

  public boolean isLastFragment() {
    return isLastFragment;
  }

  public SearchNode setLastFragment(boolean lastFragment) {
    isLastFragment = lastFragment;
    return this;
  }

  public long getEncodedSearchIndex() {
    return encodeSearchIndex(searchIndex, isLastFragment);
  }

  public static long encodeSearchIndex(long searchIndex, boolean isLastFragment) {
    if (!isLastFragment) {
      return searchIndex;
    }
    if (searchIndex == NO_CONSENSUS_INDEX) {
      return NO_CONSENSUS_INDEX_WITH_LAST_FRAGMENT;
    }
    return searchIndex | LAST_FRAGMENT_MASK;
  }

  public static long extractSearchIndex(long encodedSearchIndex) {
    if (encodedSearchIndex == NO_CONSENSUS_INDEX
        || encodedSearchIndex == NO_CONSENSUS_INDEX_WITH_LAST_FRAGMENT) {
      return NO_CONSENSUS_INDEX;
    }
    return encodedSearchIndex & ~LAST_FRAGMENT_MASK;
  }

  public static boolean isLastFragment(long encodedSearchIndex) {
    return encodedSearchIndex == NO_CONSENSUS_INDEX_WITH_LAST_FRAGMENT
        || (encodedSearchIndex != NO_CONSENSUS_INDEX
            && (encodedSearchIndex & LAST_FRAGMENT_MASK) != 0);
  }

  protected void setSearchIndexFromWAL(long encodedSearchIndex) {
    this.searchIndex = extractSearchIndex(encodedSearchIndex);
    this.isLastFragment = isLastFragment(encodedSearchIndex);
  }

  public abstract SearchNode merge(List<SearchNode> searchNodes);
}
