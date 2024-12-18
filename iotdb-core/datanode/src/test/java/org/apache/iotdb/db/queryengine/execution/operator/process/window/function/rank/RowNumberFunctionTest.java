package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.FunctionTestUtils;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class RowNumberFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  private final int[] inputs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  private final List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT64);
  private final int[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  @Test
  public void testRowNumberFunction() {
    TsBlock tsBlock = FunctionTestUtils.createTsBlockForRankFunction(inputs);
    RowNumberFunction function = new RowNumberFunction();
    PartitionExecutor partitionExecutor = FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function);

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result = tsBlockBuilder.build(
        new RunLengthEncodedColumn(
            TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(column.getLong(i), expected[i]);
    }
  }
}
