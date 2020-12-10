package org.apache.iotdb.db.tools;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.chunk.ChunkDataIterator;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class TsFileSinglePathRead {

  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    String path = "root.vehicle.d0.s0";
    if (args.length >= 2) {
      filename = args[0];
      path = args[1];
    } else if (args.length >= 1){
      filename = args[0];
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      // get the chunkMetaList of the specific path
      List<ChunkMetadata> chunkMetadataList = reader
          .getChunkMetadataList(new Path(path, true), true);
      for (ChunkMetadata metadata : chunkMetadataList) {
        System.out.println("|--[Chunk]");
        ChunkReader chunkReader = new ChunkReader(reader.readMemChunk(metadata), null);
        ChunkDataIterator chunkDataIterator = new ChunkDataIterator(chunkReader);
        while (chunkDataIterator.hasNextTimeValuePair()) {
          TimeValuePair pair = chunkDataIterator.nextTimeValuePair();
          System.out.println(
              "\t\t\ttime, value: " + pair.getTimestamp() + ", " + pair.getValue());
        }
      }
    }
  }
}



