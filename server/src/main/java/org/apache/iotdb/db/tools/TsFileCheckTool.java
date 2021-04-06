package org.apache.iotdb.db.tools;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** (1) print the total size of valid Chunks (2) print the percentage of valid bytes in a TsFile. */
public class TsFileCheckTool {
  public static void main(String[] args) throws IOException {
    // file path
    String path = "test.tsfile";
    if (args.length >= 1) {
      path = args[0];
    }

    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    List<String> devices = reader.getAllDevices();
    long totalSize = 0;
    for (String device : devices) {
      Map<String, List<ChunkMetadata>> sensorMeasurementMap =
          reader.readChunkMetadataInDevice(device);
      for (Entry<String, List<ChunkMetadata>> sensorMeasurementEntry :
          sensorMeasurementMap.entrySet()) {
        System.out.println("start to restore device: " + sensorMeasurementEntry.getKey());
        for (ChunkMetadata chunkMetadata : sensorMeasurementEntry.getValue()) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          int dataSize = chunk.getHeader().getDataSize();
          totalSize += chunkMetadata.calculateRamSize();
          totalSize += dataSize;
          System.out.println(
              "restore chunk points num: "
                  + chunkMetadata.getNumOfPoints()
                  + " startTime: "
                  + chunkMetadata.getStartTime()
                  + " endTime: "
                  + chunkMetadata.getEndTime()
                  + " dataSize: "
                  + dataSize);
        }
      }
    }
    System.out.println("valid chunk size: " + totalSize * 1.0 / 1024 / 1024 / 1024 + "GB");
    System.out.println(
        "valid chunk percentage: " + totalSize * 1.0 / new File(path).length() + "%");
    System.out.println("check file done");
  }
}
