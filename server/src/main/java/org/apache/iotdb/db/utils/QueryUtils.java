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

package org.apache.iotdb.db.utils;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import java.util.List;
import org.apache.iotdb.tsfile.utils.Pair;

public class QueryUtils {

  private QueryUtils() {
    // util class
  }

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata.
   * <br/>
   * the caller should guarantee that chunkMetaData and modifications refer to the same time series
   * paths.
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   */
  public static void modifyChunkMetaData(List<ChunkMetadata> chunkMetaData,
                                         List<Modification> modifications) {
    List<Modification> sortedModifications = sortModifications(modifications);

    for (int metaIndex = 0; metaIndex < chunkMetaData.size(); metaIndex++) {
      ChunkMetadata metaData = chunkMetaData.get(metaIndex);
      for (Modification modification : sortedModifications) {
        if (modification.getVersionNum() > metaData.getVersion()) {
          doModifyChunkMetaData(modification, metaData);
        }
      }
    }
    // remove chunks that are completely deleted
    chunkMetaData.removeIf(metaData -> {
      long lower = metaData.getStartTime();
      long upper = metaData.getEndTime();
      if (metaData.getDeleteIntervalList() != null) {
        for (Pair<Long, Long> range : metaData.getDeleteIntervalList()) {
          if (upper < range.left) {
            break;
          }
          if (range.left <= lower && lower <= range.right) {
            metaData.setModified(true);
            if (upper <= range.right) {
              return true;
            }
            lower = range.right;
          } else if (lower < range.left) {
            metaData.setModified(true);
            break;
          }
        }
      }
      return false;
    });
  }

  private static LinkedList<Modification> sortModifications(List<Modification> modifications) {
    return modifications.stream().filter(x -> x instanceof Deletion)
        .sorted(Comparator.comparingLong(mods -> ((Deletion) mods).getStartTime()))
        .collect(Collectors.toCollection(LinkedList::new));
  }

  private static void doModifyChunkMetaData(Modification modification, ChunkMetadata metaData) {
    if (modification instanceof Deletion) {
      Deletion deletion = (Deletion) modification;
      metaData.addDeletion(deletion.getStartTime(), deletion.getEndTime());
    }
  }

  // remove files that do not satisfy the filter
  public static void filterQueryDataSource(QueryDataSource queryDataSource, TsFileFilter fileFilter) {
    if (fileFilter == null) {
      return;
    }
    List<TsFileResource> seqResources = queryDataSource.getSeqResources();
    List<TsFileResource> unseqResources = queryDataSource.getUnseqResources();
    seqResources.removeIf(fileFilter::fileNotSatisfy);
    unseqResources.removeIf(fileFilter::fileNotSatisfy);
  }
}
