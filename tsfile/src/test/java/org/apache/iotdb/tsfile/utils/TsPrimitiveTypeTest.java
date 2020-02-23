package org.apache.iotdb.tsfile.utils;

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;
import org.junit.Assert;
import org.junit.Test;

public class TsPrimitiveTypeTest {

  @Test
  public void testNewAndGet() {
    TsPrimitiveType intValue = TsPrimitiveType.getByType(TSDataType.INT32, 123);
    Assert.assertEquals(new TsInt(123), intValue);
    Assert.assertEquals(123, intValue.getInt());

    TsPrimitiveType longValue = TsPrimitiveType.getByType(TSDataType.INT64, 456L);
    Assert.assertEquals(new TsLong(456), longValue);
    Assert.assertEquals(456L, longValue.getLong());

    TsPrimitiveType floatValue = TsPrimitiveType.getByType(TSDataType.FLOAT, 123f);
    Assert.assertEquals(new TsFloat(123), floatValue);
    Assert.assertEquals(123f, floatValue.getFloat(), 0.01);

    TsPrimitiveType doubleValue = TsPrimitiveType.getByType(TSDataType.DOUBLE, 456d);
    Assert.assertEquals(new TsDouble(456), doubleValue);
    Assert.assertEquals(456d, doubleValue.getDouble(), 0.01);

    TsPrimitiveType textValue = TsPrimitiveType.getByType(TSDataType.TEXT, new Binary("123"));
    Assert.assertEquals(new TsBinary(new Binary("123")), textValue);
    Assert.assertEquals(new Binary("123"), textValue.getBinary());

    TsPrimitiveType booleanValue = TsPrimitiveType.getByType(TSDataType.BOOLEAN, true);
    Assert.assertEquals(new TsBoolean(true), booleanValue);
    Assert.assertTrue(booleanValue.getBoolean());
  }

}
