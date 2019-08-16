/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * This class is used to read metadata(<code>TsFileMetaData</code> and
 * <code>TsRowGroupBlockMetaData</code>).
 */
public class TsFileMetadataUtils {

  private TsFileMetadataUtils() {

  }

  /**
   * get tsfile meta data.
   *
   * @param resource -given TsFile
   * @return -meta data
   */
  public static TsFileMetaData getTsFileMetaData(TsFileResource resource) throws IOException {
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(resource, true);
    return reader.readFileMetadata();
  }

  /**
   * get row group block meta data.
   *
   * @param resource -TsFile
   * @param seriesPath -series path
   * @param fileMetaData -tsfile meta data
   * @return -device meta data
   */
  public static TsDeviceMetadata getTsDeviceMetaData(TsFileResource resource, Path seriesPath,
      TsFileMetaData fileMetaData) throws IOException {
    if (!fileMetaData.getMeasurementSchema().containsKey(seriesPath.getMeasurement())) {
      return null;
    } else {
      // get the index information of TsDeviceMetadata
      TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(seriesPath.getDevice());
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(resource, true);
      // read TsDeviceMetadata from file
      return tsFileReader.readTsDeviceMetaData(index);
    }
  }

  /**
   * get ChunkMetaData List of measurements in sensorSet included in all ChunkGroups of this device. If
   * sensorSet is empty, then return metadata of all sensor included in this device.
   */
  public static Map<Path, List<ChunkMetaData>> getChunkMetaDataList(
      Set<String> sensorSet, TsDeviceMetadata tsDeviceMetadata) {
    Map<Path, List<ChunkMetaData>> pathToChunkMetaDataList = new ConcurrentHashMap<>();
    for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroupMetaDataList()) {
      List<ChunkMetaData> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData
          .getChunkMetaDataList();
      String deviceId = chunkGroupMetaData.getDeviceID();
      for (ChunkMetaData chunkMetaData : chunkMetaDataListInOneChunkGroup) {
        if (sensorSet.isEmpty() || sensorSet.contains(chunkMetaData.getMeasurementUid())) {
          Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
          pathToChunkMetaDataList.putIfAbsent(path, new ArrayList<>());
          chunkMetaData.setVersion(chunkGroupMetaData.getVersion());
          pathToChunkMetaDataList.get(path).add(chunkMetaData);
        }
      }
    }
    for (List<ChunkMetaData> chunkMetaDataList : pathToChunkMetaDataList.values()) {
      chunkMetaDataList.sort(Comparator.comparingLong(ChunkMetaData::getStartTime));
    }
    return pathToChunkMetaDataList;
  }

}
