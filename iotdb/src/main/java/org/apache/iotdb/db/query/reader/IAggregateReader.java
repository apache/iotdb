package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.header.PageHeader;

public interface IAggregateReader extends IBatchReader {

  /**
   * Returns meta-information of batch data. If batch data comes from memory, return null. If batch
   * data comes from page data, return pageHeader.
   */
  PageHeader nextPageHeader() throws IOException;

  void skipPageData() throws IOException;
}
