package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;

public interface IPageReader {

  BatchData getAllSatisfiedPageData() throws IOException;

  Statistics getStatistics();
}
