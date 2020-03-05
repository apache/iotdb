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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class TsFileSketchTool {

  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    String outFile = "TsFile_sketch_view.txt";
    if (args.length == 1) {
      filename = args[0];
    } else if (args.length == 2) {
      filename = args[0];
      outFile = args[1];
    }
    System.out.println("TsFile path:" + filename);
    System.out.println("Sketch save path:" + outFile);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
      long length = FSFactoryProducer.getFSFactory().getFile(filename).length();
      printlnBoth(pw,
          "-------------------------------- TsFile Sketch --------------------------------");
      printlnBoth(pw, "file path: " + filename);
      printlnBoth(pw, "file length: " + length);

      // get metadata information
      TsFileSequenceReader reader = new TsFileSequenceReader(filename);
      TsFileMetaData tsFileMetaData = reader.readFileMetadata();
      List<String> tsDeviceSortedList = tsFileMetaData.getDeviceMetaDataMap()
          .keySet()
          .stream()
          .sorted().collect(Collectors.toList());
      Map<String, List<TimeseriesMetaData>> tsDeviceTimeseriesMetaDataMap = new LinkedHashMap<>();
      Map<String, List<ChunkMetaData>> tsDeviceChunkMetaDataMap = new LinkedHashMap<>();
      for (String deviceId : tsDeviceSortedList) {
        List<TimeseriesMetaData> timeseriesMetaDataList = 
            reader.readAllTimeseriesMetaDataInDevice(deviceId)
            .values()
            .stream()
            .collect(Collectors.toList());
        tsDeviceTimeseriesMetaDataMap.put(deviceId, timeseriesMetaDataList);
        List<ChunkMetaData> chunkMetaDataListInOneDevice = 
            reader.readChunkMetadataInDevice(deviceId);
        tsDeviceChunkMetaDataMap.put(deviceId, chunkMetaDataListInOneDevice);
      }
      

      // begin print
      StringBuilder str1 = new StringBuilder();
      for (int i = 0; i < 21; i++) {
        str1.append("|");
      }

      printlnBoth(pw, "");
      printlnBoth(pw, String.format("%20s", "POSITION") + "|\tCONTENT");
      printlnBoth(pw, String.format("%20s", "--------") + " \t-------");
      printlnBoth(pw, String.format("%20d", 0) + "|\t[magic head] " + reader.readHeadMagic());
      printlnBoth(pw,
          String.format("%20d", TSFileConfig.MAGIC_STRING.getBytes().length)
              + "|\t[version number] "
              + reader.readVersionNumber());
      // device begins
      for (Entry<String, List<ChunkMetaData>> entry : tsDeviceChunkMetaDataMap.entrySet()) {
        printlnBoth(pw, str1.toString() + "\t [Device] of "+ entry.getKey() + 
            ", num of Chunks:" + entry.getValue().size());
        // chunk begins
        long chunkEndPos = 0;
        for (ChunkMetaData chunkMetaData : entry.getValue()) {
          printlnBoth(pw,
              String.format("%20d", chunkMetaData.getOffsetOfChunkHeader()) + "|\t[Chunk] of "
                  + chunkMetaData.getMeasurementUid() + ", numOfPoints:" + chunkMetaData
                  .getNumOfPoints() + ", time range:[" + chunkMetaData.getStartTime() + ","
                  + chunkMetaData.getEndTime() + "], tsDataType:" + chunkMetaData.getDataType()
                  + ", \n" + String.format("%20s", "") + " \t" + chunkMetaData.getStatistics());
          printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 1");
          printlnBoth(pw, String.format("%20s", "") + "|\t\t[ChunkHeader]");
          Chunk chunk = reader.readMemChunk(chunkMetaData);
          printlnBoth(pw,
              String.format("%20s", "") + "|\t\t" + chunk.getHeader().getNumOfPages() + " pages");
          chunkEndPos =
              chunkMetaData.getOffsetOfChunkHeader() + chunk.getHeader().getSerializedSize() + chunk
                  .getHeader().getDataSize();
        }
        // chunkGroupFooter begins
        printlnBoth(pw, String.format("%20s", chunkEndPos) + "|\t[Chunk Group Footer]");
        ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter(chunkEndPos, false);
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 0");
        printlnBoth(pw,
            String.format("%20s", "") + "|\t\t[deviceID] " + chunkGroupFooter.getDeviceID());
        printlnBoth(pw,
            String.format("%20s", "") + "|\t\t[dataSize] " + chunkGroupFooter.getDataSize());
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[num of chunks] " + chunkGroupFooter
            .getNumberOfChunks());
        printlnBoth(pw, str1.toString() + "\t[Device] of "
            + entry.getKey() + " ends");
      }

      // metadata begins
      if (tsDeviceSortedList.isEmpty()) {
        printlnBoth(pw, String.format("%20s",  reader.getFileMetadataPos() - 1)
                + "|\t[marker] 2");
      } else {
        printlnBoth(pw,
            String.format("%20s", (tsFileMetaData.getDeviceMetaDataMap()
                .get(tsDeviceSortedList.get(0))).left - 1)
                + "|\t[marker] 2");
      }
      for (Entry<String, Pair<Long,Integer>> entry 
          : tsFileMetaData.getDeviceMetaDataMap().entrySet()) {
        printlnBoth(pw,
            String.format("%20s", entry.getValue().left
                + "|\t[DeviceMetadata] of " + entry.getKey()));
      }

      printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos()) + "|\t[TsFileMetaData]");
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[num of devices] " + tsFileMetaData
              .getDeviceMetaDataMap().size());
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t" + tsFileMetaData.getDeviceMetaDataMap().size()
              + " key&TsDeviceMetadataIndex");
      // boolean createByIsNotNull = (tsFileMetaData.getCreatedBy() != null);
      // printlnBoth(pw,
      //    String.format("%20s", "") + "|\t\t[createBy isNotNull] " + createByIsNotNull);
      // if (createByIsNotNull) {
      //  printlnBoth(pw,
      //      String.format("%20s", "") + "|\t\t[createBy] " + tsFileMetaData.getCreatedBy());
      // }
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[totalChunkNum] " + tsFileMetaData.getTotalChunkNum());
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[invalidChunkNum] " + tsFileMetaData
              .getInvalidChunkNum());

      // bloom filter
      BloomFilter bloomFilter = tsFileMetaData.getBloomFilter();
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[bloom filter bit vector byte array length] "
              + bloomFilter.serialize().length);
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[bloom filter bit vector byte array] ");
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[bloom filter number of bits] "
              + bloomFilter.getSize());
      printlnBoth(pw,
          String.format("%20s", "") + "|\t\t[bloom filter number of hash functions] "
              + bloomFilter.getHashFunctionSize());

      printlnBoth(pw,
          String.format("%20s", (reader.getFileMetadataPos() + reader.getFileMetadataSize()))
              + "|\t[TsFileMetaDataSize] " + reader.getFileMetadataSize());

      printlnBoth(pw,
          String.format("%20s", reader.getFileMetadataPos() + reader.getFileMetadataSize() + 4)
              + "|\t[magic tail] " + reader.readTailMagic());

      printlnBoth(pw,
          String.format("%20s", length) + "|\tEND of TsFile");

      printlnBoth(pw, "");

      printlnBoth(pw,
          "---------------------------------- TsFile Sketch End ----------------------------------");
    }
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

}
