package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.TableWindowOperatorTestUtils;
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

public class PercentRankFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  private final int[] inputs = {1, 1, 2, 2, 3};

  private final List<TSDataType> outputDataTypes =
      Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE);
  private final double[] expected = {0, 0, 0.5, 0.5, 1};

  @Test
  public void testPercentRankFunction() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    PercentRankFunction function = new PercentRankFunction();
    List<Integer> sortedColumns = Collections.singletonList(0);
    PartitionExecutor partitionExecutor =
        FunctionTestUtils.createPartitionExecutor(tsBlock, inputDataTypes, function, sortedColumns);

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(expected.length, outputDataTypes);
    while (partitionExecutor.hasNext()) {
      partitionExecutor.processNextRow(tsBlockBuilder);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    Column column = result.getColumn(1);

    Assert.assertEquals(column.getPositionCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      // 0, 0.5 and 1 do not need delta
      Assert.assertEquals(column.getDouble(i), expected[i], 0);
    }
  }
}
