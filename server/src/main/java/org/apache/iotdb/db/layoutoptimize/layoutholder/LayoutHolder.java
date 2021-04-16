package org.apache.iotdb.db.layoutoptimize.layoutholder;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.*;

public class LayoutHolder {
  // device -> List<Measurement>
  private Map<String, List<String>> layoutMap = new HashMap<>();
  // device -> chunk size
  private Map<String, Long> chunkMap = new HashMap<>();
  private static final LayoutHolder INSTANCE = new LayoutHolder();

  public static LayoutHolder getInstance() {
    return INSTANCE;
  }

  private LayoutHolder() {}

  public void updateMetadata() {
    MManager manager = MManager.getInstance();
    List<PartialPath> storageGroupPaths = manager.getAllStorageGroupPaths();
    for (PartialPath storageGroupPath : storageGroupPaths) {
      try {
        List<PartialPath> timeSeriesPaths = manager.getAllTimeseriesPath(storageGroupPath);
        for (PartialPath timeSeriesPath : timeSeriesPaths) {
          if (!layoutMap.containsKey(timeSeriesPath.getDevice())) {
            layoutMap.put(timeSeriesPath.getDevice(), new ArrayList<>());
          }
          layoutMap
              .get(timeSeriesPath.getDevicePath().getFullPath())
              .add(timeSeriesPath.getMeasurement());
        }
      } catch (MetadataException e) {
        continue;
      }
    }

    // the measurement is in lexicographical order by default
    for (Map.Entry<String, List<String>> entry : layoutMap.entrySet()) {
      Collections.sort(entry.getValue());
    }
  }

  public List<String> getLayoutForDevice(String deviceId) {
    return layoutMap.getOrDefault(deviceId, null);
  }
}
