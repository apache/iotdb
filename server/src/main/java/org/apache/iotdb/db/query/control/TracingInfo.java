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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A distinct TracingInfo is storaged for each query id, which includes the total number of chunks,
 * the average points number of chunk, the information of sequence files and unSequence files this
 * query involves.
 */
public class TracingInfo {

  private int totalChunkNum = 0;
  private long totalChunkPoints = 0;
  private int totalPageNum = 0;
  private int overlappedPageNum = 0;
  private Set<TsFileResource> seqFileSet = new HashSet<>();
  private Set<TsFileResource> unSeqFileSet = new HashSet<>();

  public TracingInfo() {}

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  public long getTotalChunkPoints() {
    return totalChunkPoints;
  }

  public void addChunkInfo(int totalChunkNum, long totalChunkPoints) {
    this.totalChunkNum += totalChunkNum;
    this.totalChunkPoints += totalChunkPoints;
  }

  public void addTotalPageNum(int totalPageNum) {
    this.totalPageNum += totalPageNum;
  }

  public void addOverlappedPageNum() {
    this.overlappedPageNum++;
  }

  public int getTotalPageNum() {
    return totalPageNum;
  }

  public int getOverlappedPageNum() {
    return overlappedPageNum;
  }

  public Set<TsFileResource> getSeqFileSet() {
    return seqFileSet;
  }

  public Set<TsFileResource> getUnSeqFileSet() {
    return unSeqFileSet;
  }

  public void addTsFileSet(List<TsFileResource> seqResources, List<TsFileResource> unSeqResources) {
    this.seqFileSet.addAll(seqResources);
    this.unSeqFileSet.addAll(unSeqResources);
  }
}
