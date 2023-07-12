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

import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DataNodeQueryContext {
  @GuardedBy("lock")
  private final Set<PartialPath> needQueryAllRegionsForLastSet;

  private final AtomicInteger dataNodeFINum;

  private final ReentrantLock lock = new ReentrantLock();

  public DataNodeQueryContext(int dataNodeFINum) {
    this.needQueryAllRegionsForLastSet = new HashSet<>();
    this.dataNodeFINum = new AtomicInteger(dataNodeFINum);
  }

  public boolean needQueryAllRegionsForLast(PartialPath path) {
    return needQueryAllRegionsForLastSet.contains(path);
  }

  public void addNeedQueryAllRegionsForLast(PartialPath path) {
    needQueryAllRegionsForLastSet.add(path);
  }

  public int decreaseDataNodeFINum() {
    return dataNodeFINum.decrementAndGet();
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }
}
