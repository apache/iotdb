/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.utils;

import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

public class VersionUtils {

  private VersionUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void applyVersion(List<ChunkMetadata> chunkMetadataList, List<Pair<Long, Long>> versionInfo) {
    if (versionInfo == null || versionInfo.isEmpty()) {
      return;
    }
    int versionIndex = 0;
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {

      while (chunkMetadata.getOffsetOfChunkHeader() >= versionInfo.get(versionIndex).left) {
        versionIndex++;
        // When the TsFile is uncompleted,
        // skip the chunkMetadatas those don't have their version information
        if (versionIndex >= versionInfo.size()) {
          return;
        }
      }

      chunkMetadata.setVersion(versionInfo.get(versionIndex).right);
    }
  }
}
