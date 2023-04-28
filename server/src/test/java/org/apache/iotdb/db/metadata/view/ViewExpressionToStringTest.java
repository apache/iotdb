package org.apache.iotdb.db.metadata.view;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.binary.arithmetic.MultiplicationViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.db.metadata.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.metadata.view.viewExpression.leaf.TimestampViewOperand;
import org.apache.iotdb.db.metadata.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ternary.BetweenViewExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ViewExpressionToStringTest {

  @Test
  public void testTimseriesOperand() {
    String fullPath = new String("root.db.device.s01");
    TimeSeriesViewOperand timeSeriesViewOperand = new TimeSeriesViewOperand(fullPath);
    Assert.assertEquals(fullPath, timeSeriesViewOperand.toString());
    Assert.assertEquals(fullPath, timeSeriesViewOperand.toString(true));
    Assert.assertEquals(fullPath, timeSeriesViewOperand.toString(false));
  }

  @Test
  public void testAdditionViewExpression() {
    TimeSeriesViewOperand timeSeriesViewOperand = new TimeSeriesViewOperand("root.db.device.s01");
    ConstantViewOperand constantViewOperand = new ConstantViewOperand(TSDataType.INT32, "2");
    AdditionViewExpression add =
        new AdditionViewExpression(timeSeriesViewOperand, constantViewOperand);

    String expectedRoot = new String("root.db.device.s01 + 2");
    String expectedNotRoot = new String("(root.db.device.s01 + 2)");
    Assert.assertEquals(expectedRoot, add.toString());
    Assert.assertEquals(expectedRoot, add.toString(true));
    Assert.assertEquals(expectedNotRoot, add.toString(false));
  }

  @Test
  public void testTwoBinaryExpression() {
    TimeSeriesViewOperand ts1 = new TimeSeriesViewOperand("root.db.device.s01");
    ConstantViewOperand constant2 = new ConstantViewOperand(TSDataType.INT32, "2");
    AdditionViewExpression add = new AdditionViewExpression(ts1, constant2);
    TimeSeriesViewOperand ts2 = new TimeSeriesViewOperand("root.ln.d.s01");
    MultiplicationViewExpression multiplication = new MultiplicationViewExpression(add, ts2);

    String expectedRoot = new String("(root.db.device.s01 + 2) * root.ln.d.s01");
    String expectedNotRoot = new String("((root.db.device.s01 + 2) * root.ln.d.s01)");
    Assert.assertEquals(expectedRoot, multiplication.toString());
    Assert.assertEquals(expectedRoot, multiplication.toString(true));
    Assert.assertEquals(expectedNotRoot, multiplication.toString(false));
  }

  @Test
  public void testFunctionViewExpression01() {
    String functionName = "func";
    FunctionViewExpression func = new FunctionViewExpression(functionName);

    String expectedRoot = new String("func()");
    String expectedNotRoot = new String("func()");
    Assert.assertEquals(expectedRoot, func.toString());
    Assert.assertEquals(expectedRoot, func.toString(true));
    Assert.assertEquals(expectedNotRoot, func.toString(false));
  }

  @Test
  public void testFunctionViewExpression02() {
    String functionName = "MAX";
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();

    TimeSeriesViewOperand ts1 = new TimeSeriesViewOperand("root.db.device.s01");
    ConstantViewOperand constant2 = new ConstantViewOperand(TSDataType.INT32, "2");
    AdditionViewExpression add = new AdditionViewExpression(ts1, constant2);
    TimeSeriesViewOperand ts2 = new TimeSeriesViewOperand("root.ln.d.s01");
    List<ViewExpression> exps = Arrays.asList(add, ts2);

    FunctionViewExpression func = new FunctionViewExpression(functionName, keys, values, exps);

    String expectedRoot = new String("MAX(root.db.device.s01 + 2, root.ln.d.s01)");
    String expectedNotRoot = new String("MAX(root.db.device.s01 + 2, root.ln.d.s01)");
    Assert.assertEquals(expectedRoot, func.toString());
    Assert.assertEquals(expectedRoot, func.toString(true));
    Assert.assertEquals(expectedNotRoot, func.toString(false));
  }

  @Test
  public void testFunctionViewExpression03() {
    String functionName = "CAST";
    List<String> keys = Collections.singletonList("type");
    List<String> values = Collections.singletonList("INT32");

    TimeSeriesViewOperand ts2 = new TimeSeriesViewOperand("root.ln.d.s01");
    List<ViewExpression> exps = Collections.singletonList(ts2);

    FunctionViewExpression func = new FunctionViewExpression(functionName, keys, values, exps);

    String expectedRoot = new String("CAST(type=INT32)(root.ln.d.s01)");
    String expectedNotRoot = new String("CAST(type=INT32)(root.ln.d.s01)");
    Assert.assertEquals(expectedRoot, func.toString());
    Assert.assertEquals(expectedRoot, func.toString(true));
    Assert.assertEquals(expectedNotRoot, func.toString(false));
  }

  @Test
  public void testFunctionViewExpression04() {
    String functionName = "CAST";
    List<String> keys = Collections.singletonList("type");
    List<String> values = Collections.singletonList("INT32");

    TimeSeriesViewOperand ts2 = new TimeSeriesViewOperand("root.ln.d.s01");
    List<ViewExpression> exps = Collections.singletonList(ts2);

    FunctionViewExpression func = new FunctionViewExpression(functionName, keys, values, exps);

    String expectedRoot = new String("CAST(type=INT32)(root.ln.d.s01)");
    String expectedNotRoot = new String("CAST(type=INT32)(root.ln.d.s01)");
    Assert.assertEquals(expectedRoot, func.toString());
    Assert.assertEquals(expectedRoot, func.toString(true));
    Assert.assertEquals(expectedNotRoot, func.toString(false));
  }

  @Test
  public void testFunctionViewExpression05() {
    String functionName = "FUNC";
    List<String> keys = Arrays.asList("type", "key");
    List<String> values = Arrays.asList("INT32", "value");

    TimeSeriesViewOperand ts1 = new TimeSeriesViewOperand("root.db.device.s01");
    TimeSeriesViewOperand ts2 = new TimeSeriesViewOperand("root.ln.d.s01");
    List<ViewExpression> exps = Arrays.asList(ts1, ts2);

    FunctionViewExpression func = new FunctionViewExpression(functionName, keys, values, exps);

    String expectedRoot =
        new String("FUNC(type=INT32, key=value)(root.db.device.s01, root.ln.d.s01)");
    String expectedNotRoot =
        new String("FUNC(type=INT32, key=value)(root.db.device.s01, root.ln.d.s01)");
    Assert.assertEquals(expectedRoot, func.toString());
    Assert.assertEquals(expectedRoot, func.toString(true));
    Assert.assertEquals(expectedNotRoot, func.toString(false));
  }

  @Test
  public void testBetweenViewExpression() {
    TimestampViewOperand timestamp01 = new TimestampViewOperand();
    TimestampViewOperand timestamp02 = new TimestampViewOperand();
    TimeSeriesViewOperand ts1 = new TimeSeriesViewOperand("root.db.device.s01");
    BetweenViewExpression exp = new BetweenViewExpression(ts1, timestamp01, timestamp02);

    String expectedRoot = new String("root.db.device.s01 BETWEEN TIMESTAMP AND TIMESTAMP");
    String expectedNotRoot = new String("(root.db.device.s01 BETWEEN TIMESTAMP AND TIMESTAMP)");
    Assert.assertEquals(expectedRoot, exp.toString());
    Assert.assertEquals(expectedRoot, exp.toString(true));
    Assert.assertEquals(expectedNotRoot, exp.toString(false));
  }
}
