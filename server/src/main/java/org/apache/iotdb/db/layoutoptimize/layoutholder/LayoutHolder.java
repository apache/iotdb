package org.apache.iotdb.db.layoutoptimize.layoutholder;

import org.apache.iotdb.db.exception.layoutoptimize.LayoutNotExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.*;

public class LayoutHolder {
  // device -> List<Measurement>
  private Map<String, List<String>> orderMap = new HashMap<>();
  // device -> chunk size in memory
  private Map<String, Long> chunkMap = new HashMap<>();
  private static final LayoutHolder INSTANCE = new LayoutHolder();

  public static LayoutHolder getInstance() {
    return INSTANCE;
  }

  private LayoutHolder() {}

  public void updateMetadata() {
    MManager manager = MManager.getInstance();
    List<PartialPath> storageGroupPaths = manager.getAllStorageGroupPaths();
    Set<String> deviceUpdated = new HashSet<>();
    for (PartialPath storageGroupPath : storageGroupPaths) {
      try {
        List<PartialPath> timeSeriesPaths = manager.getAllTimeseriesPath(storageGroupPath);
        for (PartialPath timeSeriesPath : timeSeriesPaths) {
          if (!orderMap.containsKey(timeSeriesPath.getDevice())) {
            orderMap.put(timeSeriesPath.getDevice(), new ArrayList<>());
          }
          if (!orderMap
              .get(timeSeriesPath.getDevicePath().getFullPath())
              .contains(timeSeriesPath.getMeasurement())) {
            orderMap
                .get(timeSeriesPath.getDevicePath().getFullPath())
                .add(timeSeriesPath.getMeasurement());
            deviceUpdated.add(timeSeriesPath.getDevicePath().getFullPath());
          }
        }
      } catch (MetadataException e) {
        continue;
      }
    }

    // the measurement is in lexicographical order by default
    for (String device : deviceUpdated) {
      Collections.sort(orderMap.get(device));
    }
  }

  public Pair<List<String>, Long> getLayoutForDevice(String deviceId)
      throws LayoutNotExistException {
    if (!orderMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    List<String> measurementOrder = new ArrayList<>(orderMap.get(deviceId));
    long chunkSize = chunkMap.getOrDefault(deviceId, 0L);
    return new Pair<>(measurementOrder, chunkSize);
  }

  public List<String> getMeasurementForDevice(String deviceId) throws LayoutNotExistException {
    if (!orderMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    return new ArrayList<>(orderMap.get(deviceId));
  }

  public long getChunkSize(String deviceId) throws LayoutNotExistException {
    if (!orderMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    return chunkMap.get(deviceId);
  }
}
