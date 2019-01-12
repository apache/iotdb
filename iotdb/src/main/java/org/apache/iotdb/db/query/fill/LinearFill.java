package org.apache.iotdb.db.query.fill;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.exception.PathErrorException;

import java.io.IOException;



public class LinearFill extends IFill{

  private long beforeRange, afterRange;

  private Path path;

  private BatchData result;

  public LinearFill(long beforeRange, long afterRange) {
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  public LinearFill(Path path, TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
    super(dataType, queryTime);
    this.path = path;
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
    result = new BatchData(dataType, true, true);
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  public void setBeforeRange(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  public long getAfterRange() {
    return afterRange;
  }

  public void setAfterRange(long afterRange) {
    this.afterRange = afterRange;
  }

  @Override
  public IFill copy(Path path) {
    return new LinearFill(path, dataType, queryTime, beforeRange, afterRange);
  }

  @Override
  public BatchData getFillResult() throws ProcessorException, IOException, PathErrorException {
    long beforeTime, afterTime;
    if (beforeRange == -1) {
      beforeTime = 0;
    } else {
      beforeTime = queryTime - beforeRange;
    }
    if (afterRange == -1) {
      afterTime = Long.MAX_VALUE;
    } else {
      afterTime = queryTime + afterRange;
    }

    return result;
  }
}