/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

public class QueryUtils {

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata.
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   * @return a list of all generated ModifiedChunkMetadata.
   */
  public static void modifyChunkMetaData(List<ChunkMetaData> chunkMetaData,
                                         List<Modification> modifications) {
    int modIndex = 0;

    for (int metaIndex = 0; metaIndex < chunkMetaData.size(); metaIndex++) {
      ChunkMetaData metaData = chunkMetaData.get(metaIndex);
      for (int j = modIndex; j < modifications.size(); j++) {
        // iterate each modification to find the max deletion time
        Modification modification = modifications.get(modIndex);
        if (modification.getVersionNum() > metaData.getVersion()) {
          if (modification instanceof Deletion) {
            Deletion deletion = (Deletion) modification;
            if (metaData.getDeletedAt() < deletion.getTimestamp()) {
              metaData.setDeletedAt(deletion.getTimestamp());
              modIndex = j;
            }
          }
        } else {
          // skip old modifications for next metadata
          modIndex++;
        }
      }
    }
    // remove chunks that are completely deleted
    chunkMetaData.removeIf(metaData -> metaData.getDeletedAt() >= metaData.getEndTime());
  }
}
