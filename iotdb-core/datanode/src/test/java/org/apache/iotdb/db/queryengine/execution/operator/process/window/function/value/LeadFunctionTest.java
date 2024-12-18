package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

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

public class LeadFunctionTest {
  private final List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);
  // Inputs element less than 0 means this pos is null
  private final int[] inputs = {0, -1, -1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1, -1, -1};

  private final List<TSDataType> outputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT32);

  @Test
  public void testLeadFunctionIgnoreNullWithoutDefault() {
    int[] expected = {2, 2, 2, 3, 4, 4, 5, 6, 6, -1, -1, -1, -1, -1, -1, -1};

    TsBlock tsBlock = FunctionTestUtils.createTsBlockForValueFunction(inputs);
    LeadFunction function = new LeadFunction(0, 2, null, true);
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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }

  @Test
  public void testLagFunctionIgnoreNullWithDefault() {
    int[] expected = {2, 2, 2, 3, 4, 4, 5, 6, 6, 10, 10, 10, 10, 10, 10, 10};

    TsBlock tsBlock = FunctionTestUtils.createTsBlockForValueFunction(inputs);
    LeadFunction function = new LeadFunction(0, 2, 10, true);
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
      Assert.assertEquals(expected[i], column.getInt(i));
    }
  }

  @Test
  public void testLagFunctionNotIgnoreNullWithoutDefault() {
    int[] expected = {-1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1, -1, -1, -1, -1};

    TsBlock tsBlock = FunctionTestUtils.createTsBlockForValueFunction(inputs);
    LeadFunction function = new LeadFunction(0, 2, null, false);
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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }

  @Test
  public void testLagFunctionNotIgnoreNullWithDefault() {
    int[] expected = {-1, 1, 2, -1, 3, 4, -1, 5, 6, -1, -1, -1, -1, -1, 10, 10};

    TsBlock tsBlock = FunctionTestUtils.createTsBlockForValueFunction(inputs);
    LeadFunction function = new LeadFunction(0, 2, 10, false);
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
      if (expected[i] < 0) {
        Assert.assertTrue(column.isNull(i));
      } else {
        Assert.assertEquals(expected[i], column.getInt(i));
      }
    }
  }
}
