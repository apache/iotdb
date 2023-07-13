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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DataNodeQueryContext {
  // left of Pair is DataNodeSeriesScanNum, right of Pair is the last value waiting to be updated
  @GuardedBy("lock")
  private final Map<PartialPath, Pair<AtomicInteger, TimeValuePair>> uncachedPathToSeriesScanInfo;

  private final AtomicInteger dataNodeFINum;

  private final ReentrantLock lock = new ReentrantLock();

  public DataNodeQueryContext(int dataNodeFINum) {
    this.uncachedPathToSeriesScanInfo = new HashMap<>();
    this.dataNodeFINum = new AtomicInteger(dataNodeFINum);
  }

  public boolean unCached(PartialPath path) {
    return uncachedPathToSeriesScanInfo.containsKey(path);
  }

  public void addUnCachePath(PartialPath path, AtomicInteger dataNodeSeriesScanNum) {
    uncachedPathToSeriesScanInfo.put(path, new Pair<>(dataNodeSeriesScanNum, null));
  }

  public AtomicInteger getDataNodeSeriesScanNum(PartialPath path) {
    return uncachedPathToSeriesScanInfo.get(path).left;
  }

  public Pair<AtomicInteger, TimeValuePair> getSeriesScanInfo(PartialPath path) {
    return uncachedPathToSeriesScanInfo.get(path);
  }

  public int decreaseDataNodeFINum() {
    return dataNodeFINum.decrementAndGet();
  }

  public void lock() {
    lock.lock();
  }

  public void unLock() {
    lock.unlock();
  }
}
