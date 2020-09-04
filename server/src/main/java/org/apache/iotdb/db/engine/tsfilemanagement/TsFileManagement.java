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

package org.apache.iotdb.db.engine.tsfilemanagement;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseHotCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public abstract class TsFileManagement {

  protected String storageGroupName;
  protected String storageGroupDir;
  /**
   * hotCompactionMergeLock is used to wait for TsFile list change in hot compaction merge
   * processor.
   */
  public final ReadWriteLock hotCompactionMergeLock = new ReentrantReadWriteLock();

  public TsFileManagement(String storageGroupName, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
  }

  /**
   * get the TsFile list which has been completed hot compacted
   */
  public abstract List<TsFileResource> getStableTsFileList(boolean sequence);

  /**
   * get the TsFile list in sequence
   */
  public abstract List<TsFileResource> getTsFileList(boolean sequence);

  /**
   * get the TsFile list iterator in sequence
   */
  public abstract Iterator<TsFileResource> getIterator(boolean sequence);

  /**
   * remove one TsFile from list
   */
  public abstract void remove(TsFileResource tsFileResource, boolean sequence);

  /**
   * remove some TsFiles from list
   */
  public abstract void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  /**
   * add one TsFile to list
   */
  public abstract void add(TsFileResource tsFileResource, boolean sequence);

  /**
   * add some TsFiles to list
   */
  public abstract void addAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  /**
   * is one TsFile contained in list
   */
  public abstract boolean contains(TsFileResource tsFileResource, boolean sequence);

  /**
   * clear list
   */
  public abstract void clear();

  /**
   * is the list empty
   */
  public abstract boolean isEmpty(boolean sequence);

  /**
   * return TsFile list size
   */
  public abstract int size(boolean sequence);

  /**
   * recover TsFile list
   */
  public abstract void recover();

  /**
   * fork current TsFile list (call this before merge)
   */
  public abstract void forkCurrentFileList(long timePartition);

  protected abstract void merge(long timePartition);

  public class HotCompactionMergeTask implements Runnable {

    private CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack;
    private long timePartitionId;

    public HotCompactionMergeTask(CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack,
        long timePartitionId) {
      this.closeHotCompactionMergeCallBack = closeHotCompactionMergeCallBack;
      this.timePartitionId = timePartitionId;
    }

    @Override
    public void run() {
      merge(timePartitionId);
      closeHotCompactionMergeCallBack.call();
    }
  }
}
