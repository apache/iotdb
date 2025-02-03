package org.apache.iotdb.library.dquality.util;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import java.io.IOException;

public class SubsequenceRowIterator implements RowIterator {
  private RowIterator originalIterator = null;
  private long start = 0;
  private long end = 0;
  private int currentIndex;
  private boolean resetFlag;

  public SubsequenceRowIterator(RowIterator originalIterator, long start, long end) {
    this.originalIterator = originalIterator;
    this.start = start;
    this.end = end;
    this.currentIndex = -1;
    this.resetFlag = true;
    reset();
  }

  @Override
  public boolean hasNextRow() {
    return currentIndex < end && originalIterator.hasNextRow();
  }

  @Override
  public Row next() throws IOException {
    currentIndex++;
    if (currentIndex < start) {
      while (currentIndex < start && originalIterator.hasNextRow()) {
        originalIterator.next();
        currentIndex++;
      }
    }
    if (currentIndex >= start && currentIndex <= end) {
      return originalIterator.next();
    }
    throw new IOException("No more rows in the specified range");
  }

  @Override
  public void reset() {
    originalIterator.reset();
    currentIndex = -1;
    resetFlag = true;
    while (resetFlag && currentIndex < start - 1) {
      try {
        originalIterator.next();
        currentIndex++;
      } catch (IOException e) {
        resetFlag = false;
      }
    }
  }
}
