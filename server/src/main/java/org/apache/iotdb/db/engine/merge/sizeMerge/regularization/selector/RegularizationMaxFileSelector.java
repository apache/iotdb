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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization.selector;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.engine.merge.sizeMerge.BaseSizeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * RegularizationMaxFileSelector selects the most files from given seqFiles which can be merged as
 * files with all devices' max and min time larger than given time block
 */
public class RegularizationMaxFileSelector extends BaseSizeFileSelector {

  public RegularizationMaxFileSelector(Collection<TsFileResource> seqFiles, long budget) {
    super(seqFiles, budget, Long.MIN_VALUE);
  }

  public RegularizationMaxFileSelector(Collection<TsFileResource> seqFiles, long budget,
      long timeLowerBound) {
    super(seqFiles, budget, timeLowerBound);
  }

  protected boolean isSmallFile(TsFileResource seqFile) throws IOException {
    List<Path> paths = resource.getFileReader(seqFile).getAllPaths();
    for (Path currentPath : paths) {
      List<ChunkMetadata> chunkMetadataList;
      try {
        chunkMetadataList = resource
            .queryChunkMetadata(new PartialPath(currentPath.getFullPath()), seqFile);
      } catch (IllegalPathException e) {
        throw new IOException(e);
      }
      int cnt = 0;
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        if (cnt != chunkMetadataList.size() - 1 && !this.mergeSizeSelectorStrategy
            .isChunkEnoughLarge(chunkMetadata, minChunkPointNum, timeBlock)) {
          return true;
        }
        cnt++;
      }
    }
    return false;
  }
}
