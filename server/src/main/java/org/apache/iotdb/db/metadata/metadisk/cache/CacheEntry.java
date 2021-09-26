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
package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheEntry {

  CacheEntry pre = null;
  CacheEntry next = null;
  IMNode value;

  /** whether the node in memory cache has been modified. default value is true */
  boolean isModified = true;

  AtomicInteger semaphore = new AtomicInteger(0);

  CacheEntry(IMNode mNode) {
    value = mNode;
    mNode.setCacheEntry(this);
  }

  CacheEntry getPre() {
    return pre;
  }

  CacheEntry getNext() {
    return next;
  }

  void setPre(CacheEntry pre) {
    this.pre = pre;
  }

  void setNext(CacheEntry next) {
    this.next = next;
  }

  public IMNode getMNode() {
    return value;
  }

  public void setMNode(IMNode mNode) {
    value = mNode;
  }

  public boolean isModified() {
    return isModified;
  }

  public void setModified(boolean modified) {
    isModified = modified;
  }

  public boolean isLocked() {
    return semaphore.get() > 0;
  }

  public void increaseLock() {
    semaphore.getAndIncrement();
  }

  public void decreaseLock() {
    if (semaphore.get() > 0) {
      semaphore.getAndDecrement();
    }
  }
}
