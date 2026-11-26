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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.queryengine.plan.planner.memory.FakedMemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.read.filter.basic.Filter;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FakedFragmentInstanceContext extends FragmentInstanceContext {

  // Retain the registration-time state: an unclosed file may be sealed before the query finishes.
  private final Set<TsFileResource> closedResources = new HashSet<>();
  private final Set<TsFileResource> unclosedResources = new HashSet<>();

  public FakedFragmentInstanceContext(Filter timeFilter, DataRegion dataRegion) {
    super(0, new FakedMemoryReservationManager(), timeFilter, dataRegion, false, false);
  }

  public QueryDataSource getSharedQueryDataSource(IFullPath sourcePath)
      throws QueryProcessException {
    if (sharedQueryDataSource == null) {
      initQueryDataSource(sourcePath);
    }
    return (QueryDataSource) sharedQueryDataSource;
  }

  public void initQueryDataSource(IFullPath sourcePath) throws QueryProcessException {

    dataRegion.tryReadLock(Long.MAX_VALUE);
    try {
      try {
        this.sharedQueryDataSource =
            dataRegion.query(
                Collections.singletonList(sourcePath),
                sourcePath.getDeviceId(),
                this,
                getGlobalTimeFilter(),
                null,
                Long.MAX_VALUE);

        // used files should be added before mergeLock is unlocked, or they may be deleted by
        // running merge
        if (sharedQueryDataSource != null) {
          ((QueryDataSource) sharedQueryDataSource).setSingleDevice(true);
          List<TsFileResource> tsFileList =
              ((QueryDataSource) sharedQueryDataSource).getSeqResources();
          if (tsFileList != null) {
            for (TsFileResource tsFile : tsFileList) {
              addFilePathToMap(tsFile, tsFile.isClosed(), closedResources, unclosedResources);
            }
          }
          tsFileList = ((QueryDataSource) sharedQueryDataSource).getUnseqResources();
          if (tsFileList != null) {
            for (TsFileResource tsFile : tsFileList) {
              addFilePathToMap(tsFile, tsFile.isClosed(), closedResources, unclosedResources);
            }
          }
        }
      } finally {
        dataRegion.readUnlock();
      }
    } catch (RuntimeException | Error e) {
      // Rollback can close readers or wait for another close; never retain the region lock here.
      try {
        releaseSharedQueryDataSource();
      } catch (RuntimeException | Error rollbackFailure) {
        if (rollbackFailure != e) {
          e.addSuppressed(rollbackFailure);
        }
      }
      throw e;
    }
  }

  public void releaseSharedQueryDataSource() {
    try (DeferredFileReaderReleases releases = new DeferredFileReaderReleases()) {
      releases.addAll(closedResources, true);
      releases.addAll(unclosedResources, false);
    } finally {
      sharedQueryDataSource = null;
    }
  }

  @Override
  protected boolean checkIfModificationExists(TsFileResource tsFileResource) {
    return false;
  }
}
