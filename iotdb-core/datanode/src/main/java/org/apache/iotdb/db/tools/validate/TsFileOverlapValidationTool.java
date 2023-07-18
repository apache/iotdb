package org.apache.iotdb.db.tools.validate;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileOverlapValidationTool {

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      return;
    }
    String absolutePath = args[0];
    validate(absolutePath);
  }

  @SuppressWarnings("java:S3776")
  private static void validate(String path) throws IOException {
    File seqRoot = new File(path);
    if (!seqRoot.isDirectory()) {
      return;
    }
    for (File storageGroup : Objects.requireNonNull(seqRoot.listFiles())) {
      if (!storageGroup.isDirectory()) {
        continue;
      }
      for (File dataRegion : Objects.requireNonNull(storageGroup.listFiles())) {
        if (!dataRegion.isDirectory()) {
          continue;
        }
        for (File timePartition : Objects.requireNonNull(dataRegion.listFiles())) {
          if (!timePartition.isDirectory()) {
            continue;
          }
          List<TsFileResource> resources = loadAndSortFile(timePartition.getAbsolutePath());
          validateTsFileResources(resources);
        }
      }
    }
  }

  private static List<TsFileResource> loadAndSortFile(String timePartitionPath) throws IOException {
    File dir = new File(timePartitionPath);
    // get all seq file resources under the time partition dir
    List<File> tsFiles =
        Arrays.asList(
            Objects.requireNonNull(
                dir.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))));
    // sort the seq files with timestamp
    tsFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[0]),
                  Long.parseLong(f2.getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getName().split("-")[1]),
                  Long.parseLong(f2.getName().split("-")[1]))
              : timeDiff;
        });
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (File tsFile : tsFiles) {
      TsFileResource resource =
          new TsFileResource(SystemFileFactory.INSTANCE.getFile(tsFile.getAbsolutePath()));
      resource.deserialize();
      tsFileResources.add(resource);
    }
    return tsFileResources;
  }

  private static Set<TsFileResource> validateTsFileResources(List<TsFileResource> resources)
      throws IOException {
    // deviceID -> <TsFileResource, last end time>
    Map<String, Pair<TsFileResource, Long>> lastEndTimeMap = new HashMap<>();
    Set<TsFileResource> overlapFiles = new HashSet<>();
    for (TsFileResource resource : resources) {
      DeviceTimeIndex timeIndex;
      if (resource.getTimeIndexType() != 1) {
        // if time index is not device time index, then deserialize it from resource file
        timeIndex = resource.buildDeviceTimeIndex();
      } else {
        timeIndex = (DeviceTimeIndex) resource.getTimeIndex();
      }
      Set<String> devices = timeIndex.getDevices();
      for (String device : devices) {
        long currentStartTime = timeIndex.getStartTime(device);
        long currentEndTime = timeIndex.getEndTime(device);
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          overlapFiles.add(resource);
          System.out.println(
              "Add File " + resource.getTsFile().getAbsolutePath() + " to overlap file list");
          break;
        }
        lastDeviceInfo.left = resource;
        lastDeviceInfo.right = currentEndTime;
        lastEndTimeMap.put(device, lastDeviceInfo);
      }
    }
    return overlapFiles;
  }
}
