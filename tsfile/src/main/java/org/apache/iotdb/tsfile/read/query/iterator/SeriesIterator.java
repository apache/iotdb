package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SeriesIterator extends BaseTimeSeries {

  private final LeafNode leafNode;

  public SeriesIterator(TSDataType dataType, IBatchReader reader, Filter filter) {
    super(Collections.singletonList(dataType));
    leafNode = new LeafNode(reader);
  }

  public SeriesIterator(IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(Arrays.asList(chunkMetadataList.get(0).getDataType()));
    IBatchReader reader = new FileSeriesReader(chunkLoader, chunkMetadataList, filter);

    leafNode = new LeafNode(reader);
  }

  @Override
  public boolean hasNext() {
    try {
      return leafNode.hasNext();
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public Object[] next() {
    long ts = 0;
    try {
      ts = leafNode.next();
    } catch (IOException e) {
      return new Object[0];
    }
    Object value = leafNode.currentValue();
    return new Object[]{ts, value};
  }
}
