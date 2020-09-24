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
