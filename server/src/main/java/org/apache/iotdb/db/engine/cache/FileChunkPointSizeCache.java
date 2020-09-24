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
package org.apache.iotdb.db.engine.cache;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChunkPointSizeCache {

  private static final Logger logger = LoggerFactory.getLogger(FileChunkPointSizeCache.class);

  // (absolute path,avg chunk point size)
  private Map<String, Map<String, Long>> tsfileDeviceChunkPointCache;

  private FileChunkPointSizeCache() {
    tsfileDeviceChunkPointCache = new HashMap<>();
  }

  public static FileChunkPointSizeCache getInstance() {
    return FileChunkPointSizeCacheHolder.INSTANCE;
  }

  public Map<String, Long> get(File tsfile) {
    String path = tsfile.getAbsolutePath();
    return tsfileDeviceChunkPointCache.computeIfAbsent(path, k -> {
      Map<String, Long> deviceChunkPointMap = new HashMap<>();
      try {
        if (tsfile.exists()) {
          TsFileSequenceReader reader = new TsFileSequenceReader(path);
          List<Path> pathList = reader.getAllPaths();
          for (Path sensorPath : pathList) {
            String device = sensorPath.getDevice();
            long chunkPointNum = deviceChunkPointMap.getOrDefault(device, 0L);
            List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(sensorPath);
            for (ChunkMetadata chunkMetadata : chunkMetadataList) {
              chunkPointNum += chunkMetadata.getNumOfPoints();
            }
            deviceChunkPointMap.put(device, chunkPointNum);
          }
        } else {
          logger.info("{} tsfile does not exist", path);
        }
      } catch (IOException e) {
        logger.error(
            "{} tsfile reader creates error", path, e);
      }
      return deviceChunkPointMap;
    });
  }

  public void put(String tsfilePath, Map<String, Long> deviceChunkPointMap) {
    tsfileDeviceChunkPointCache.put(tsfilePath, deviceChunkPointMap);
  }

  /**
   * singleton pattern.
   */
  private static class FileChunkPointSizeCacheHolder {

    private static final FileChunkPointSizeCache INSTANCE = new FileChunkPointSizeCache();
  }
}
