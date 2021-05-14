package org.apache.iotdb.db.layoutoptimize.layoutholder;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.db.exception.layoutoptimize.LayoutNotExistException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.layoutoptimize.estimator.DataSizeEstimator;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class LayoutHolder {
  private static final Logger logger = LoggerFactory.getLogger(LayoutHolder.class);
  // device -> layout
  private Map<String, Layout> layoutMap = new HashMap<>();
  private static final LayoutHolder INSTANCE = new LayoutHolder();

  public static LayoutHolder getInstance() {
    return INSTANCE;
  }

  private LayoutHolder() {
    loadLayout();
  }

  /** Update metadata from {@link MManager} */
  public void updateMetadata() {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    MManager manager = MManager.getInstance();
    List<PartialPath> storageGroupPaths = manager.getAllStorageGroupPaths();
    Set<String> deviceUpdated = new HashSet<>();
    for (PartialPath storageGroupPath : storageGroupPaths) {
      try {
        List<PartialPath> timeSeriesPaths = manager.getAllTimeseriesPath(storageGroupPath);
        for (PartialPath timeSeriesPath : timeSeriesPaths) {
          if (!layoutMap.containsKey(timeSeriesPath.getDevice())) {
            layoutMap.put(timeSeriesPath.getDevice(), new Layout());
          }
          if (!layoutMap
              .get(timeSeriesPath.getDevicePath().getFullPath())
              .measurements
              .contains(timeSeriesPath.getMeasurement())) {
            layoutMap
                .get(timeSeriesPath.getDevicePath().getFullPath())
                .measurements
                .add(timeSeriesPath.getMeasurement());
            deviceUpdated.add(timeSeriesPath.getDevicePath().getFullPath());
          }
        }
      } catch (MetadataException e) {
        continue;
      }
    }

    // the measurement is in lexicographical order by default
    // the default chunk size is set according to default avg series threshold
    long defaultAvgSeriesPointNum =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    MManager mmanager = MManager.getInstance();
    DataSizeEstimator estimator = DataSizeEstimator.getInstance();
    for (String device : deviceUpdated) {
      Layout curLayout = layoutMap.get(device);
      Collections.sort(curLayout.measurements);
      if (curLayout.averageChunkSize == 0) {
        try {
          PartialPath devicePath = new PartialPath(device);
          PartialPath storageGroupPath = mmanager.getStorageGroupPath(devicePath);
          curLayout.averageChunkSize =
              estimator.getChunkSizeInDisk(
                  storageGroupPath.getFullPath(), defaultAvgSeriesPointNum);
        } catch (IllegalPathException
            | StorageGroupNotSetException
            | DataSizeInfoNotExistsException e) {
        }
      }
    }
  }

  /**
   * store the layout in layout holder
   *
   * @param device the device id of the layout, must be full path
   * @param measurementOrder the order of the measurements in this device
   * @param chunkSize the average chunk size of this device
   */
  public void setLayout(String device, List<String> measurementOrder, long chunkSize) {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    layoutMap.put(device, new Layout(measurementOrder, chunkSize));
    persistLayout();
  }

  /**
   * get the layout for device
   *
   * @param deviceId the id of the device, must be full path
   * @return the pair of < Order of measurements, AverageChunkSize>
   * @throws LayoutNotExistException
   */
  public Pair<List<String>, Long> getLayoutForDevice(String deviceId)
      throws LayoutNotExistException {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    if (!layoutMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    List<String> measurementOrder = new ArrayList<>(layoutMap.get(deviceId).measurements);
    long chunkSize = layoutMap.get(deviceId).averageChunkSize;
    return new Pair<>(measurementOrder, chunkSize);
  }

  /**
   * get the measurement order for device
   *
   * @param deviceId the id of the device, must be full path
   * @return the list of measurements
   * @throws LayoutNotExistException
   */
  public List<String> getMeasurementForDevice(String deviceId) throws LayoutNotExistException {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    if (!layoutMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    return new ArrayList<>(layoutMap.get(deviceId).measurements);
  }

  /**
   * get the chunk size for device
   *
   * @param deviceId the id of the device, must be full path
   * @return the average chunk size of the device
   * @throws LayoutNotExistException
   */
  public long getChunkSize(String deviceId) throws LayoutNotExistException {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    if (!layoutMap.containsKey(deviceId))
      throw new LayoutNotExistException(String.format("layout for %s not exists", deviceId));
    return layoutMap.get(deviceId).averageChunkSize;
  }

  /**
   * set the device as the currently flushing device, the memtable size threshold will be changed
   *
   * @param device the id of the device, must be full path
   * @throws StorageGroupNotSetException
   * @throws LayoutNotExistException
   */
  public void setDeviceForFlush(PartialPath device)
      throws StorageGroupNotSetException, LayoutNotExistException {
    if (layoutMap == null) {
      if (!loadLayout()) {
        layoutMap = new HashMap<>();
      }
    }
    if (!layoutMap.containsKey(device.getFullPath())) {
      throw new LayoutNotExistException(
          String.format("Layout for %s not exists", device.getFullPath()));
    }
    Layout layout = layoutMap.get(device.getFullPath());
    long chunkSize = layout.averageChunkSize * layout.measurements.size();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(chunkSize);
  }

  /**
   * persist the layout in local file
   *
   * @return true if success to persist layout, else false
   */
  public boolean persistLayout() {
    if (layoutMap == null) {
      return true;
    }
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.setPrettyPrinting();
    Gson gson = gsonBuilder.create();
    String json = gson.toJson(layoutMap);
    File layoutDir = new File(IoTDBDescriptor.getInstance().getConfig().getLayoutDir());
    if (!layoutDir.exists()) {
      if (!layoutDir.mkdir()) {
        return false;
      }
    }
    File layoutFile = new File(layoutDir.getPath() + File.separator + "layout.json");
    try {
      if (!layoutFile.exists()) {
        if (!layoutFile.createNewFile()) {
          logger.error("failed to create file {}", layoutFile);
          return false;
        }
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(layoutFile));
      writer.write(json);
      writer.flush();
      writer.close();
    } catch (IOException e) {
      logger.error("failed to persist layout");
      return false;
    }
    logger.info("persist layout to {}", layoutFile);
    return true;
  }

  /**
   * load layout from local file
   *
   * @return true if success to load layout, else false
   */
  public boolean loadLayout() {
    File layoutDir = new File(IoTDBDescriptor.getInstance().getConfig().getLayoutDir());
    if (!layoutDir.exists()) {
      logger.info("fail to load layout");
      return false;
    }
    File layoutFile = new File(layoutDir.getPath() + File.separator + "layout.json");
    if (!layoutFile.exists()) {
      logger.info("fail to load layout");
      return false;
    }
    try {
      Scanner scanner = new Scanner(new FileInputStream(layoutFile));
      StringBuilder sb = new StringBuilder();
      while (scanner.hasNextLine()) {
        sb.append(scanner.nextLine());
      }
      String json = sb.toString();
      Gson gson = new Gson();
      Map<String, Map<String, Object>> jsonObject = gson.fromJson(json, layoutMap.getClass());
      for (String key : jsonObject.keySet()) {
        Map<String, Object> layout = jsonObject.get(key);
        layoutMap.put(
            key,
            new Layout(
                (ArrayList<String>) layout.get("measurements"),
                ((Double) layout.get("averageChunkSize")).longValue()));
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    logger.info("load layout from local file successfully");
    return true;
  }

  public boolean useLayout(String device) {
    if (!layoutMap.containsKey(device)) {
      logger.info(
          "fail to use layout of {}, because LayoutHolder does not contain the layout for it",
          device);
      return false;
    }
    long averageChunkSize = layoutMap.get(device).averageChunkSize;
    DataSizeEstimator estimator = DataSizeEstimator.getInstance();
    MManager manager = MManager.getInstance();
    try {
      PartialPath path = new PartialPath(device);
      PartialPath storageGroup = manager.getStorageGroupPath(path);
      long avgPointNum = estimator.getPointNumInDisk(storageGroup.getFullPath(), averageChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold((int) avgPointNum);
      logger.info(
          "successfully use the layout for {}, the avg point num is {}", device, avgPointNum);
      return true;
    } catch (IllegalPathException
        | StorageGroupNotSetException
        | DataSizeInfoNotExistsException e) {
      logger.info("fail to use layout for {}", device);
      return false;
    }
  }

  public boolean hasLayoutForDevice(String deviceID) {
    return layoutMap.containsKey(deviceID);
  }
}
