package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class UnseqCompactionReader {

  private PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  public UnseqCompactionReader(
      List<TsFileResource> unseqResources,
      Map<TsFileResource, TsFileSequenceReader> readerMap,
      PartialPath timeseriesPath)
      throws IOException {
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.getStartTime(), o2.getStartTime());
              return timeCompare != 0
                  ? timeCompare
                  : Integer.compare(o1.getPriority(), o2.getPriority());
            });
    // get all chunkMetadatas from unseq files
    int priority = 0;
    for (TsFileResource unseqResource : unseqResources) {
      TsFileSequenceReader reader = readerMap.get(unseqResource);
      for (ChunkMetadata chunkMetadata : reader.getChunkMetadataList(timeseriesPath, true)) {
        chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, priority++));
      }
    }
  }

  class ChunkMetadataElement {
    ChunkMetadata chunkMetadata;
    int priority;

    public ChunkMetadataElement(ChunkMetadata chunkMetadata, int priority) {
      this.chunkMetadata = chunkMetadata;
      this.priority = priority;
    }

    public long getStartTime() {
      return chunkMetadata.getStartTime();
    }

    public int getPriority() {
      return priority;
    }
  }
}
