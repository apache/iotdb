package org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator;

import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.io.IOException;

public interface RowListForwardIterator extends ListForwardIterator {
  Column[] currentBlock() throws IOException;
}
