package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;

public class MergeAggregateReader implements IAggregateReader {

  private PriorityQueue<Element> heap = new PriorityQueue<>(
      Comparator.comparingLong(o -> o.currentPageHeader.getStartTime()));

  public MergeAggregateReader(List<IAggregateReader> readerList) throws IOException {
    init(readerList);
  }

  private void init(List<IAggregateReader> readerList) throws IOException {
    for (IAggregateReader aggregateReader : readerList) {
      if (aggregateReader.hasNextBatch()) {
        Element element = new Element(aggregateReader, aggregateReader.nextPageHeader());
        heap.add(element);
      }
    }
  }

  @Override
  public PageHeader nextPageHeader() {
    return heap.peek() != null ? heap.peek().currentPageHeader : null;
  }

  @Override
  public void skipPageData() throws IOException {
    Element top = heap.poll();
    if (top != null) {
      top.reader.skipPageData();
      if (top.reader.hasNextBatch()) {
        top.currentPageHeader = top.reader.nextPageHeader();
        heap.add(top);
      }
    }
  }

  @Override
  public boolean hasNextBatch() {
    return !heap.isEmpty();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    Element top = heap.poll();
    BatchData batchData = null;
    if (top != null) {
      batchData = top.reader.nextBatch();
      if (top.reader.hasNextBatch()) {
        top.currentPageHeader = top.reader.nextPageHeader();
        heap.add(top);
      }
    }
    return batchData;
  }

  @Override
  public void close() throws IOException {
    Element element;
    while ((element = heap.poll()) != null) {
      element.reader.close();
    }
  }

  static class Element {

    IAggregateReader reader;
    PageHeader currentPageHeader;


    Element(IAggregateReader reader, PageHeader currentPageHeader) {
      this.reader = reader;
      this.currentPageHeader = currentPageHeader;
    }
  }
}
