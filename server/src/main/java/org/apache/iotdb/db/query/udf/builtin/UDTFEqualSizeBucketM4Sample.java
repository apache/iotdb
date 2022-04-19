package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.Arrays;

public class UDTFEqualSizeBucketM4Sample extends UDTFEqualSizeBucketSample {

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    bucketSize *= 4;
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(dataType);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws UDFException, IOException {
    if (rowWindow.windowSize() < 3) {
      throw new UDFException("The bucket size is too small for M4 sampling.");
    }
    switch (dataType) {
      case INT32:
        transformInt(rowWindow, collector);
        break;
      case INT64:
        transformLong(rowWindow, collector);
        break;
      case FLOAT:
        transformFloat(rowWindow, collector);
        break;
      case DOUBLE:
        transformDouble(rowWindow, collector);
        break;
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  public void transformInt(RowWindow rowWindow, PointCollector collector) throws IOException {
    int minIndex = 1, maxIndex = 1;
    int maxValue = rowWindow.getRow(1).getInt(0);
    int minValue = rowWindow.getRow(1).getInt(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
      int value = rowWindow.getRow(i).getInt(0);
      if (minValue > value) {
        minValue = value;
        minIndex = i;
      }
      if (maxValue < value) {
        maxValue = value;
        maxIndex = i;
      }
    }
    int[] arr = new int[] {0, minIndex, maxIndex, rowWindow.windowSize() - 1};
    Arrays.sort(arr);
    Row row;
    for (int i = 0; i < 4; i++) {
      row = rowWindow.getRow(arr[i]);
      collector.putInt(row.getTime(), row.getInt(0));
    }
  }

  public void transformLong(RowWindow rowWindow, PointCollector collector) throws IOException {
    int minIndex = 1, maxIndex = 1;
    long maxValue = rowWindow.getRow(1).getLong(0);
    long minValue = rowWindow.getRow(1).getLong(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
      long value = rowWindow.getRow(i).getLong(0);
      if (minValue > value) {
        minValue = value;
        minIndex = i;
      }
      if (maxValue < value) {
        maxValue = value;
        maxIndex = i;
      }
    }
    int[] arr = new int[] {0, minIndex, maxIndex, rowWindow.windowSize() - 1};
    Arrays.sort(arr);
    Row row;
    for (int i = 0; i < 4; i++) {
      row = rowWindow.getRow(arr[i]);
      collector.putLong(row.getTime(), row.getLong(0));
    }
  }

  public void transformFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
    int minIndex = 1, maxIndex = 1;
    float maxValue = rowWindow.getRow(1).getFloat(0);
    float minValue = rowWindow.getRow(1).getFloat(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
      float value = rowWindow.getRow(i).getFloat(0);
      if (minValue > value) {
        minValue = value;
        minIndex = i;
      }
      if (maxValue < value) {
        maxValue = value;
        maxIndex = i;
      }
    }
    int[] arr = new int[] {0, minIndex, maxIndex, rowWindow.windowSize() - 1};
    Arrays.sort(arr);
    Row row;
    for (int i = 0; i < 4; i++) {
      row = rowWindow.getRow(arr[i]);
      collector.putFloat(row.getTime(), row.getFloat(0));
    }
  }

  public void transformDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
    int minIndex = 1, maxIndex = 1;
    double maxValue = rowWindow.getRow(1).getDouble(0);
    double minValue = rowWindow.getRow(1).getDouble(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
      double value = rowWindow.getRow(i).getDouble(0);
      if (minValue > value) {
        minValue = value;
        minIndex = i;
      }
      if (maxValue < value) {
        maxValue = value;
        maxIndex = i;
      }
    }
    int[] arr = new int[] {0, minIndex, maxIndex, rowWindow.windowSize() - 1};
    Arrays.sort(arr);
    Row row;
    for (int i = 0; i < 4; i++) {
      row = rowWindow.getRow(arr[i]);
      collector.putDouble(row.getTime(), row.getDouble(0));
    }
  }
}
