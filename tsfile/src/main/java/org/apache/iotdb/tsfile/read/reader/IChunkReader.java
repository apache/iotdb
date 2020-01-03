package org.apache.iotdb.tsfile.read.reader;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;

public interface IChunkReader {

  boolean hasNextSatisfiedPage() throws IOException;

  BatchData nextPageData() throws IOException;

  PageHeader nextPageHeader();

  void skipPageData();

  void close() throws IOException;
}
