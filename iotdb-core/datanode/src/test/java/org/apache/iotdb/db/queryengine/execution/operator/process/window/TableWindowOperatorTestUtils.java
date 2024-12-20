package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperatorTestUtils {
  public static TsBlock createIntsTsBlockWithoutNulls(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithNulls(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }
}
