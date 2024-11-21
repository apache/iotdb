package org.apache.iotdb.db.protocol.rest.table.v1.handler;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.protocol.rest.table.v1.model.InsertTabletRequest;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

public class StatementConstructionHandler {

  private static final DataNodeDevicePathCache DEVICE_PATH_CACHE =
      DataNodeDevicePathCache.getInstance();

  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletReq)
      throws IllegalPathException, WriteProcessRejectException {
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(insertTabletReq.getTable()));
    insertStatement.setMeasurements(insertTabletReq.getColumnNames().toArray(new String[0]));
    long[] timestamps =
        insertTabletReq.getTimestamps().stream().mapToLong(Long::longValue).toArray();
    if (timestamps.length != 0) {
      TimestampPrecisionUtils.checkTimestampPrecision(timestamps[timestamps.length - 1]);
    }
    insertStatement.setTimes(timestamps);
    int columnSize = insertTabletReq.getColumnNames().size();
    int rowSize = insertTabletReq.getTimestamps().size();
    List<List<Object>> rawData = insertTabletReq.getValues();
    Object[] columns = new Object[columnSize];
    BitMap[] bitMaps = new BitMap[columnSize];
    List<String> rawDataType = insertTabletReq.getDataTypes();
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      bitMaps[columnIndex] = new BitMap(rowSize);
      switch (dataTypes[columnIndex]) {
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(rowIndex).get(columnIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              if ("1".equals(data.toString())) {
                booleanValues[rowIndex] = true;
              } else if ("0".equals(data.toString())) {
                booleanValues[rowIndex] = false;
              } else {
                booleanValues[rowIndex] = (Boolean) data;
              }
            }
          }
          columns[columnIndex] = booleanValues;
          break;
        case INT32:
        case DATE:
          int[] intValues = new int[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(rowIndex).get(columnIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              intValues[rowIndex] = (int) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = new long[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(rowIndex).get(columnIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              longValues[rowIndex] = (int) object;
            } else if (object instanceof Long) {
              longValues[rowIndex] = (long) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(rowIndex).get(columnIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              floatValues[rowIndex] = Float.parseFloat(String.valueOf(data));
            }
          }
          columns[columnIndex] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(rowIndex).get(columnIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              doubleValues[rowIndex] =
                  Double.parseDouble(String.valueOf(rawData.get(rowIndex).get(columnIndex)));
            }
          }
          columns[columnIndex] = doubleValues;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary[] binaryValues = new Binary[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(rowIndex).get(columnIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
              binaryValues[rowIndex] = new Binary("".getBytes(StandardCharsets.UTF_8));
            } else {
              binaryValues[rowIndex] =
                  new Binary(
                      rawData
                          .get(rowIndex)
                          .get(columnIndex)
                          .toString()
                          .getBytes(StandardCharsets.UTF_8));
            }
          }
          columns[columnIndex] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Invalid input: " + rawDataType.get(columnIndex));
      }
    }
    /*for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
            switch (dataTypes[columnIndex]) {
                case BOOLEAN:
                    boolean[] booleanValues = new boolean[rowSize];
                    Object dataBool = rawData.get(rowIndex).get(columnIndex);
                    if (dataBool == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                    }else{
                        if ("1".equals(dataBool.toString())) {
                            booleanValues[rowIndex] = true;
                        } else if ("0".equals(dataBool.toString())) {
                            booleanValues[rowIndex] = false;
                        } else {
                            booleanValues[rowIndex] = (Boolean) dataBool;
                        }
                    }
                    columns[columnIndex] = booleanValues;
                    break;
                case INT32:
                case DATE:
                    int[] intValues = new int[rowSize];
                    Object objectInt = rawData.get(columnIndex).get(rowIndex);
                    if (objectInt == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                    } else if (objectInt instanceof Integer) {
                        intValues[rowIndex] = (int) objectInt;
                    } else {
                        throw new WriteProcessRejectException(
                                "unsupported data type: " + objectInt.getClass().toString());
                    }
                    columns[columnIndex] = intValues;
                    break;
                case INT64:
                case TIMESTAMP:
                    long[] longValues = new long[rowSize];
                    Object objectLong = rawData.get(columnIndex).get(rowIndex);
                    if (objectLong == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                    } else if (objectLong instanceof Integer) {
                        longValues[rowIndex] = (int) objectLong;
                    } else if (objectLong instanceof Long) {
                        longValues[rowIndex] = (long) objectLong;
                    } else {
                        throw new WriteProcessRejectException(
                                "unsupported data type: " + objectLong.getClass().toString());
                    }
                    columns[columnIndex] = longValues;
                    break;
                case FLOAT:
                    float[] floatValues = new float[rowSize];
                    Object data = rawData.get(columnIndex).get(rowIndex);
                    if (data == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                    } else {
                        floatValues[rowIndex] = Float.parseFloat(String.valueOf(data));
                    }
                    columns[columnIndex] = floatValues;
                    break;
                case DOUBLE:
                    double[] doubleValues = new double[rowSize];
                    if (rawData.get(columnIndex).get(rowIndex) == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                    } else {
                        doubleValues[rowIndex] =
                                Double.parseDouble(String.valueOf(rawData.get(columnIndex).get(rowIndex)));
                    }
                    columns[columnIndex] = doubleValues;
                    break;
                case TEXT:
                case BLOB:
                case STRING:
                    Binary[] binaryValues = new Binary[rowSize];
                    if (rawData.get(columnIndex).get(rowIndex) == null) {
                        bitMaps[columnIndex].mark(rowIndex);
                        binaryValues[rowIndex] = new Binary("".getBytes(StandardCharsets.UTF_8));
                    } else {
                        binaryValues[rowIndex] =
                                new Binary(
                                        rawData
                                                .get(columnIndex)
                                                .get(rowIndex)
                                                .toString()
                                                .getBytes(StandardCharsets.UTF_8));
                    }
                    columns[columnIndex] = binaryValues;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid input: " + rawDataType.get(columnIndex));
            }
        }
    }*/

    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(rowSize);
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(false);
    insertStatement.setWriteToTable(true);
    TsTableColumnCategory[] columnCategories =
        new TsTableColumnCategory[insertTabletReq.getColumnTypes().size()];
    for (int i = 0; i < columnCategories.length; i++) {
      columnCategories[i] =
          TsTableColumnCategory.fromTsFileColumnType(
              Tablet.ColumnType.valueOf(insertTabletReq.getColumnTypes().get(i)));
    }
    insertStatement.setColumnCategories(columnCategories);

    return insertStatement;
  }
}
