/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeLastPointTabletEvent;
import org.apache.iotdb.db.pipe.event.common.tsblock.PipeLastPointTsBlockEvent;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipeLastPointTsBlockEventTest {

  public TsBlock createTsBlock() {
    long[] timeArray = {1L, 2L, 3L, 4L, 5L};
    boolean[] booleanValueArray = {true, false, false, false, true};
    boolean[] booleanIsNull = {true, true, false, true, false};
    int[] intValueArray = {10, 20, 30, 40, 50};
    boolean[] intIsNull = {false, true, false, false, true};
    long[] longValueArray = {100L, 200L, 300L, 400, 500L};
    boolean[] longIsNull = {true, false, false, true, true};
    float[] floatValueArray = {1000.0f, 2000.0f, 3000.0f, 4000.0f, 5000.0f};
    boolean[] floatIsNull = {false, false, true, true, false};
    double[] doubleValueArray = {10000.0, 20000.0, 30000.0, 40000.0, 50000.0};
    boolean[] doubleIsNull = {true, false, false, true, false};
    Binary[] binaryValueArray = {
      new Binary("19970909", TSFileConfig.STRING_CHARSET),
      new Binary("ty", TSFileConfig.STRING_CHARSET),
      new Binary("love", TSFileConfig.STRING_CHARSET),
      new Binary("zm", TSFileConfig.STRING_CHARSET),
      new Binary("19950421", TSFileConfig.STRING_CHARSET)
    };
    boolean[] binaryIsNull = {false, false, false, false, false};

    TsBlockBuilder builder = new TsBlockBuilder(getMeasurementDataTypes());
    for (int i = 0; i < timeArray.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timeArray[i]);
      if (booleanIsNull[i]) {
        builder.getColumnBuilder(0).appendNull();
      } else {
        builder.getColumnBuilder(0).writeBoolean(booleanValueArray[i]);
      }
      if (intIsNull[i]) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeInt(intValueArray[i]);
      }
      if (longIsNull[i]) {
        builder.getColumnBuilder(2).appendNull();
      } else {
        builder.getColumnBuilder(2).writeLong(longValueArray[i]);
      }
      if (floatIsNull[i]) {
        builder.getColumnBuilder(3).appendNull();
      } else {
        builder.getColumnBuilder(3).writeFloat(floatValueArray[i]);
      }
      if (doubleIsNull[i]) {
        builder.getColumnBuilder(4).appendNull();
      } else {
        builder.getColumnBuilder(4).writeDouble(doubleValueArray[i]);
      }
      if (binaryIsNull[i]) {
        builder.getColumnBuilder(5).appendNull();
      } else {
        builder.getColumnBuilder(5).writeBinary(binaryValueArray[i]);
      }
      builder.declarePosition();
    }
    return builder.build();
  }

  private String[] getMeasurementIdentifiers() {
    return new String[] {"s1", "s2", "s3", "s4", "s5", "s6"};
  }

  private List<TSDataType> getMeasurementDataTypes() {
    return Arrays.asList(
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT);
  }

  private List<MeasurementSchema> createMeasurementSchemas() {
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    final String[] measurementIds = getMeasurementIdentifiers();
    final List<TSDataType> dataTypes = getMeasurementDataTypes();

    for (int i = 0; i < measurementIds.length; i++) {
      measurementSchemas.add(new MeasurementSchema(measurementIds[i], dataTypes.get(i)));
    }

    return measurementSchemas;
  }

  private PartialPath createPartialPath() throws IllegalPathException {
    return new PartialPath(DeviceIDFactory.getInstance().getDeviceID("root.ts"));
  }

  private PipeLastPointTsBlockEvent generatePipeLastPointEvent(
      TsBlock tsBlock, PartialPath partialPath, List<MeasurementSchema> measurementSchemas)
      throws IllegalPathException {
    PipeLastPointTsBlockEvent pipeLastPointTsBlockEvent =
        new PipeLastPointTsBlockEvent(
            tsBlock,
            System.currentTimeMillis(),
            partialPath,
            measurementSchemas,
            "test",
            System.currentTimeMillis(),
            null,
            null,
            0L,
            1L);

    return pipeLastPointTsBlockEvent;
  }

  @Test
  public void convertToPipeLastPointTabletEventTest() throws IllegalPathException {
    TsBlock tsBlock = createTsBlock();
    PartialPath partialPath = createPartialPath();
    List<MeasurementSchema> measurementSchemas = createMeasurementSchemas();
    PipeLastPointTsBlockEvent tsBlockEvent =
        generatePipeLastPointEvent(tsBlock, partialPath, measurementSchemas);

    PipeLastPointTabletEvent lastPointTabletEvent =
        tsBlockEvent.convertToPipeLastPointTabletEvent(
            null, (bitMap, block, deviceId, schemas) -> {});

    Tablet tablet = lastPointTabletEvent.getTablet();

    Object[] values = tablet.values;
    int rowNum = tsBlock.getTimeColumn().getPositionCount();
    int columnNum = values.length;
    for (int row = 0; row < rowNum; row++) {
      Assert.assertEquals(tablet.timestamps[row], tsBlock.getTimeColumn().getLong(row));
      for (int column = 0; column < columnNum; column++) {
        Column tsColumn = tsBlock.getColumn(column);
        BitMap bitMap = tablet.bitMaps[column];
        Assert.assertEquals(tsColumn.isNull(row), bitMap.isMarked(row));
        if (tsColumn.isNull(row)) {
          continue;
        }
        switch (tsColumn.getDataType()) {
          case BOOLEAN:
            Assert.assertEquals(tsColumn.getBoolean(row), ((boolean[]) values[column])[row]);
            break;
          case INT32:
            Assert.assertEquals(tsColumn.getInt(row), ((int[]) values[column])[row]);
            break;
          case DATE:
            Assert.assertEquals(
                DateUtils.parseIntToLocalDate(tsColumn.getInt(row)),
                ((LocalDate[]) values[column])[row]);
            break;
          case INT64:
          case TIMESTAMP:
            Assert.assertEquals(tsColumn.getLong(row), ((long[]) values[column])[row]);
            break;
          case FLOAT:
            Assert.assertEquals(
                Float.compare(tsColumn.getFloat(row), ((float[]) values[column])[row]), 0);
            break;
          case DOUBLE:
            Assert.assertEquals(
                Double.compare(tsColumn.getDouble(row), ((double[]) values[column])[row]), 0);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            Assert.assertEquals(tsColumn.getBinary(row), ((Binary[]) values[column])[row]);
            break;
        }
      }
    }
  }
}
