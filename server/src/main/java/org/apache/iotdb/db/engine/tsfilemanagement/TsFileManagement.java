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
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseHotCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public abstract class TsFileManagement {

  protected String storageGroupName;
  protected String storageGroupDir;

  public TsFileManagement(String storageGroupName, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
  }

  public abstract List<TsFileResource> getMergeTsFileList(boolean sequence);

  public abstract List<TsFileResource> getTsFileList(boolean sequence);

  public abstract Iterator<TsFileResource> getIterator(boolean sequence);

  public abstract void remove(TsFileResource tsFileResource, boolean sequence);

  public abstract void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  public abstract void add(TsFileResource tsFileResource, boolean sequence);

  public abstract void addAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  public abstract void addMerged(TsFileResource tsFileResource, boolean sequence);

  public abstract void addMergedAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  public abstract boolean contains(TsFileResource tsFileResource, boolean sequence);

  public abstract void clear();

  public abstract boolean isEmpty(boolean sequence);

  public abstract int size(boolean sequence);

  public abstract void recover();

  public abstract void forkCurrentFileList();

  protected abstract void merge(ReadWriteLock hotCompactionMergeLock);

  public class HotCompactionMergeTask implements Runnable {

    private ReadWriteLock hotCompactionMergeLock;
    private CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack;

    public HotCompactionMergeTask(ReadWriteLock hotCompactionMergeLock,
        CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack) {
      this.hotCompactionMergeLock = hotCompactionMergeLock;
      this.closeHotCompactionMergeCallBack = closeHotCompactionMergeCallBack;
    }

    @Override
    public void run() {
      merge(hotCompactionMergeLock);
      closeHotCompactionMergeCallBack.call();
    }
  }
}
