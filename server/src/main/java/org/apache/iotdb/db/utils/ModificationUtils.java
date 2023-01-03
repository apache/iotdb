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

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;

public class ModificationUtils {

  private ModificationUtils() {
    // util class
  }

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata. <br>
   * the caller should guarantee that chunkMetaData and modifications refer to the same time series
   * paths.
   *
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void modifyChunkMetaData(
      List<? extends IChunkMetadata> chunkMetaData, List<Modification> modifications) {
    for (IChunkMetadata metaData : chunkMetaData) {
      for (Modification modification : modifications) {
        // When the chunkMetadata come from an old TsFile, the method modification.getFileOffset()
        // is gerVersionNum actually. In this case, we compare the versions of modification and
        // mataData to determine whether need to do modify.
        if (metaData.isFromOldTsFile()) {
          if (modification.getFileOffset() > metaData.getVersion()) {
            doModifyChunkMetaData(modification, metaData);
          }
          continue;
        }
        // The case modification.getFileOffset() == metaData.getOffsetOfChunkHeader()
        // is not supposed to exist as getFileOffset() is offset containing full chunk,
        // while getOffsetOfChunkHeader() returns the chunk header offset
        if (modification.getFileOffset() > metaData.getOffsetOfChunkHeader()) {
          doModifyChunkMetaData(modification, metaData);
        }
      }
    }
    // remove chunks that are completely deleted
    chunkMetaData.removeIf(
        metaData -> {
          if (metaData.getDeleteIntervalList() != null) {
            for (TimeRange range : metaData.getDeleteIntervalList()) {
              if (range.contains(metaData.getStartTime(), metaData.getEndTime())) {
                return true;
              } else {
                if (range.overlaps(new TimeRange(metaData.getStartTime(), metaData.getEndTime()))) {
                  metaData.setModified(true);
                }
              }
            }
          }
          return false;
        });
  }

  public static void modifyAlignedChunkMetaData(
      List<AlignedChunkMetadata> chunkMetaData, List<List<Modification>> modifications) {
    for (AlignedChunkMetadata metaData : chunkMetaData) {
      List<IChunkMetadata> valueChunkMetadataList = metaData.getValueChunkMetadataList();
      // deal with each sub sensor
      for (int i = 0; i < valueChunkMetadataList.size(); i++) {
        IChunkMetadata v = valueChunkMetadataList.get(i);
        if (v != null) {
          List<Modification> modificationList = modifications.get(i);
          for (Modification modification : modificationList) {
            // The case modification.getFileOffset() == metaData.getOffsetOfChunkHeader()
            // is not supposed to exist as getFileOffset() is offset containing full chunk,
            // while getOffsetOfChunkHeader() returns the chunk header offset
            if (modification.getFileOffset() > v.getOffsetOfChunkHeader()) {
              doModifyChunkMetaData(modification, v);
            }
          }
        }
      }
    }
    // if all sub sensors' chunk metadata are deleted, then remove the aligned chunk metadata
    // otherwise, set the deleted chunk metadata of some sensors to null
    chunkMetaData.removeIf(
        alignedChunkMetadata -> {
          // the whole aligned path need to be removed, only set to be true if all the sub sensors
          // are deleted
          boolean removed = true;
          // the whole aligned path is modified, set to be true if any sub sensor is modified
          boolean modified = false;
          List<IChunkMetadata> valueChunkMetadataList =
              alignedChunkMetadata.getValueChunkMetadataList();
          for (int i = 0; i < valueChunkMetadataList.size(); i++) {
            IChunkMetadata valueChunkMetadata = valueChunkMetadataList.get(i);
            if (valueChunkMetadata == null) {
              continue;
            }
            // current sub sensor's chunk metadata is completely removed
            boolean currentRemoved = false;
            if (valueChunkMetadata.getDeleteIntervalList() != null) {
              for (TimeRange range : valueChunkMetadata.getDeleteIntervalList()) {
                if (range.contains(
                    valueChunkMetadata.getStartTime(), valueChunkMetadata.getEndTime())) {
                  valueChunkMetadataList.set(i, null);
                  currentRemoved = true;
                  break;
                } else {
                  if (range.overlaps(
                      new TimeRange(
                          valueChunkMetadata.getStartTime(), valueChunkMetadata.getEndTime()))) {
                    valueChunkMetadata.setModified(true);
                    modified = true;
                  }
                }
              }
            }
            // current sub sensor's chunk metadata is not completely removed,
            // so the whole aligned path don't need to be removed from list
            if (!currentRemoved) {
              removed = false;
            }
          }
          alignedChunkMetadata.setModified(modified);
          return removed;
        });
  }

  private static void doModifyChunkMetaData(Modification modification, IChunkMetadata metaData) {
    if (modification instanceof Deletion) {
      Deletion deletion = (Deletion) modification;
      metaData.insertIntoSortedDeletions(deletion.getTimeRange());
    }
  }
}
