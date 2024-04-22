package org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.io.IOException;

public interface TVListForwardIterator extends ListForwardIterator {
  TimeColumn currentTimes() throws IOException;

  Column currentValues() throws IOException;
}
