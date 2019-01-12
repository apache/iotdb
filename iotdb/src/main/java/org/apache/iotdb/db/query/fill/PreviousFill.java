package org.apache.iotdb.db.query.fill;


import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.exception.PathErrorException;

import java.io.IOException;


public class PreviousFill extends IFill {

  private long beforeRange;

  private Path path;

  private BatchData result;

  public PreviousFill(Path path, TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.path = path;
    this.beforeRange = beforeRange;
    result = new BatchData(dataType, true, true);
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy(Path path) {
    return new PreviousFill(path, dataType, queryTime, beforeRange);
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  @Override
  public BatchData getFillResult() throws ProcessorException, IOException, PathErrorException {
    long beforeTime;
    if (beforeRange == -1) {
      beforeTime = 0;
    } else {
      beforeTime = queryTime - beforeRange;
    }
    return result;
  }
}
