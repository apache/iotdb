/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.read.filter;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET;

public class TsBlockFilterTest {

  private static final int COL_NUM = 3;
  private static final int ROW_NUM = 7;

  private TsBlock tsBlock;

  private TimeColumn timeColumn;
  private Column[] valueColumn;
  private boolean[] selection = new boolean[COL_NUM];

  public int getMeasurementId(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return 0;
      case INT32:
        return 1;
      case INT64:
        return 2;
      case FLOAT:
        return 3;
      case DOUBLE:
        return 4;
      case TEXT:
        return 5;
      default:
        return -1;
    }
  }

  @Before
  public void before() {
    valueColumn = new Column[ROW_NUM];
    Arrays.fill(selection, true);

    long[] timestamps = new long[] {100L, 101L, 102L};
    boolean[] valueIsNull = new boolean[] {false, false, false};
    boolean[] booleans = new boolean[] {true, false, true};
    int[] ints = new int[] {0, 100, -100};
    long[] longs = new long[] {0L, 100L, -100L};
    float[] floats = new float[] {0.0f, -100.1f, 100.1f};
    double[] doubles = new double[] {0.0d, -100.2d, 100.2d};
    Binary[] binaries =
        new Binary[] {
          new Binary("a", STRING_CHARSET),
          new Binary(null, STRING_CHARSET),
          new Binary("c", STRING_CHARSET),
        };

    BooleanColumn booleanColumn =
        new BooleanColumn(booleans.length, Optional.of(valueIsNull), booleans);
    IntColumn intColumn = new IntColumn(ints.length, Optional.of(valueIsNull), ints);
    LongColumn longColumn = new LongColumn(longs.length, Optional.of(valueIsNull), longs);
    FloatColumn floatColumn = new FloatColumn(floats.length, Optional.of(valueIsNull), floats);
    DoubleColumn doubleColumn = new DoubleColumn(doubles.length, Optional.of(valueIsNull), doubles);
    BinaryColumn binaryColumn =
        new BinaryColumn(
            binaries.length, Optional.of(new boolean[] {false, true, false}), binaries);
    BinaryColumn nullColumn =
        new BinaryColumn(
            3,
            Optional.of(new boolean[] {true, false, true}),
            new Binary[] {null, new Binary("a", STRING_CHARSET), null});

    timeColumn = new TimeColumn(timestamps.length, timestamps);
    valueColumn[getMeasurementId(TSDataType.BOOLEAN)] = booleanColumn;
    valueColumn[getMeasurementId(TSDataType.INT32)] = intColumn;
    valueColumn[getMeasurementId(TSDataType.INT64)] = longColumn;
    valueColumn[getMeasurementId(TSDataType.FLOAT)] = floatColumn;
    valueColumn[getMeasurementId(TSDataType.DOUBLE)] = doubleColumn;
    valueColumn[getMeasurementId(TSDataType.TEXT)] = binaryColumn;
    valueColumn[ROW_NUM - 1] = nullColumn;
    tsBlock = new TsBlock(timeColumn, valueColumn);
  }

  @Test
  public void testBoolean() {
    Filter booleanFilter =
        ValueFilterApi.eq(getMeasurementId(TSDataType.BOOLEAN), true, TSDataType.BOOLEAN);
    boolean[] expected = new boolean[] {true, false, true};
    Assert.assertArrayEquals(expected, booleanFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testInteger() {
    Filter intFilter = ValueFilterApi.eq(getMeasurementId(TSDataType.INT32), 100, TSDataType.INT32);
    boolean[] expected = new boolean[] {false, true, false};
    Assert.assertArrayEquals(expected, intFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testLong() {
    Filter longFilter =
        ValueFilterApi.eq(getMeasurementId(TSDataType.INT64), 100L, TSDataType.INT64);
    boolean[] expected = new boolean[] {false, true, false};
    Assert.assertArrayEquals(expected, longFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testFloat() {
    Filter floatFilter =
        ValueFilterApi.eq(getMeasurementId(TSDataType.FLOAT), -100.1f, TSDataType.FLOAT);
    boolean[] expected = new boolean[] {false, true, false};
    Assert.assertArrayEquals(expected, floatFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testDouble() {
    Filter doubleFilter =
        ValueFilterApi.eq(getMeasurementId(TSDataType.DOUBLE), -100.2d, TSDataType.DOUBLE);
    boolean[] expected = new boolean[] {false, true, false};
    Assert.assertArrayEquals(expected, doubleFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testBinary() {
    Filter binaryFilter =
        ValueFilterApi.eq(
            getMeasurementId(TSDataType.TEXT), new Binary("a", STRING_CHARSET), TSDataType.TEXT);
    boolean[] expected = new boolean[] {true, false, false};
    Assert.assertArrayEquals(expected, binaryFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testString() {
    Filter stringFilter =
        ValueFilterApi.eq(
            getMeasurementId(TSDataType.TEXT), new Binary("a", STRING_CHARSET), TSDataType.STRING);
    boolean[] expected = new boolean[] {true, false, false};
    Assert.assertArrayEquals(expected, stringFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testNull() {
    Filter nullFilter = ValueFilterApi.isNull(ROW_NUM - 1);
    boolean[] expected = new boolean[] {true, false, true};
    Assert.assertArrayEquals(expected, nullFilter.satisfyTsBlock(selection, tsBlock));
  }

  @Test
  public void testNotNull() {
    Filter notNullFilter = ValueFilterApi.isNotNull(ROW_NUM - 1);
    boolean[] expected = new boolean[] {false, true, false};
    Assert.assertArrayEquals(expected, notNullFilter.satisfyTsBlock(selection, tsBlock));
  }
}
