package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeSet;
import org.apache.iotdb.db.query.reader.PageReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月15日
 */
public class PriorityChunkMergeReader implements IBatchReader {

  final PriorityMergeReader mergeReader = new PriorityMergeReader();
  final TSDataType dataType;

  public PriorityChunkMergeReader(TSDataType dataType) {
    this.dataType = dataType;
  }

  private class Element {

    long version;
    IChunkReader reader;
    PageHeader pageHeader;
    BatchData pageData;
    TimeValuePair timeValuePair;
  }


  PriorityQueue<Element> chunkReaders = new PriorityQueue<>((o1, o2) -> {
    return Long.compare(o1.version, o2.version);
  });

  @Override
  public boolean hasNextBatch() throws IOException {
    return !chunkReaders.isEmpty();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return null;
  }

  //  @Override
  public BatchData nextBatch(long time) throws IOException {
    Element reader = getNoneOverlappedPage();
    if (reader == null) {
      return nextOverlappedPage(time);
    }
    return reader.pageData;
  }


  private BatchData nextOverlappedPage(long time) throws IOException {
    chunkReaders.forEach(
        element -> {
          try {
            mergeReader
                .addReader(new PageReader(element.pageData, element.pageHeader, element.version),
                    element.version);
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    BatchData batchData = new BatchData(dataType);
    while (mergeReader.hasNext()) {
      TimeValuePair timeValuePair = mergeReader.current();
      if (timeValuePair.getTimestamp() > time) {
        break;
      }
      batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
      mergeReader.next();
    }
    return batchData;
  }

  private Element getNoneOverlappedPage() throws IOException {
    TreeSet<Element> set = new TreeSet<>(
        (Comparator.comparingLong(o -> o.pageHeader.getStartTime())));
    chunkReaders.forEach(element -> set.add(element));

    Element element = set.pollFirst();
    while (!set.isEmpty()) {
      Element next = set.pollFirst();
      if (next.pageHeader.getStartTime() < element.pageHeader.getEndTime()) {
        return null;
      }
    }
    return element;
  }

  @Override
  public void close() throws IOException {

  }
}
