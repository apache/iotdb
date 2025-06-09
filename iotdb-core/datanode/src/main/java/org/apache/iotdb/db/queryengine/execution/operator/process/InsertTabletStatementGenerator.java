package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Arrays;
import java.util.List;

public abstract class InsertTabletStatementGenerator {
  protected int rowLimit;

  protected PartialPath devicePath;
  protected boolean isAligned;
  protected String[] measurements;
  protected TSDataType[] dataTypes;
  protected InputLocation[] inputLocations;

  protected int rowCount = 0;

  protected long[] times;
  protected Object[] columns;
  protected BitMap[] bitMaps;

  protected List<Type> sourceTypeConvertors;

  public void reset() {
    this.rowCount = 0;
    this.times = new long[rowLimit];
    this.columns = new Object[this.measurements.length];
    for (int i = 0; i < this.measurements.length; i++) {
      switch (dataTypes[i]) {
        case BOOLEAN:
          columns[i] = new boolean[rowLimit];
          break;
        case INT32:
        case DATE:
          columns[i] = new int[rowLimit];
          break;
        case INT64:
        case TIMESTAMP:
          columns[i] = new long[rowLimit];
          break;
        case FLOAT:
          columns[i] = new float[rowLimit];
          break;
        case DOUBLE:
          columns[i] = new double[rowLimit];
          break;
        case TEXT:
        case STRING:
        case BLOB:
          columns[i] = new Binary[rowLimit];
          Arrays.fill((Binary[]) columns[i], Binary.EMPTY_VALUE);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypes[i]));
      }
    }
    this.bitMaps = new BitMap[this.measurements.length];
    for (int i = 0; i < this.bitMaps.length; ++i) {
      this.bitMaps[i] = new BitMap(rowLimit);
      this.bitMaps[i].markAll();
    }
  }

  public InsertTabletStatement constructInsertTabletStatement() {
    InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
    insertTabletStatement.setDevicePath(devicePath);
    insertTabletStatement.setAligned(isAligned);
    insertTabletStatement.setMeasurements(measurements);
    insertTabletStatement.setDataTypes(dataTypes);
    insertTabletStatement.setRowCount(rowCount);

    if (rowCount != rowLimit) {
      times = Arrays.copyOf(times, rowCount);
      for (int i = 0; i < columns.length; i++) {
        bitMaps[i] = bitMaps[i].getRegion(0, rowCount);
        switch (dataTypes[i]) {
          case BOOLEAN:
            columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
            break;
          case INT32:
          case DATE:
            columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
            break;
          case INT64:
          case TIMESTAMP:
            columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
            break;
          case FLOAT:
            columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
            break;
          case DOUBLE:
            columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            columns[i] = Arrays.copyOf((Binary[]) columns[i], rowCount);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes[i]));
        }
      }
    }

    insertTabletStatement.setTimes(times);
    insertTabletStatement.setBitMaps(bitMaps);
    insertTabletStatement.setColumns(columns);

    return insertTabletStatement;
  }

  public boolean isFull() {
    return rowCount == rowLimit;
  }

  public boolean isEmpty() {
    return rowCount == 0;
  }

  public String getDevice() {
    return devicePath.toString();
  }

  public abstract int getWrittenCount();

  public abstract int getWrittenCount(String measurement);

  public abstract int processTsBlock(TsBlock tsBlock, int lastReadIndex);
}
