package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;

public abstract class UDTFContinuouslySatisfy implements UDTF {
  protected Long min;
  protected Long max;
  protected Long defaultMax = Long.MAX_VALUE;
  protected Long defaultMin = 0L;
  protected TSDataType dataType;
  protected Long satisfyValueCount;
  protected Long satisfyValueLastTime;
  protected Long satisfyValueStartTime;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    setDefaultValue();
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.BOOLEAN)
        .validate(
            args -> (Long) args[1] >= (Long) args[0],
            "max can not be smaller than min.",
            validator.getParameters().getLongOrDefault("min", defaultMin),
            validator.getParameters().getLongOrDefault("max", defaultMax))
        .validate(
            min -> (Long) min >= defaultMin,
            "min can not be smaller than " + defaultMin + ".",
            validator.getParameters().getLongOrDefault("min", defaultMin));
  }

  protected abstract void setDefaultValue();

  protected void setDefaultMax(Long defaultMax) {
    this.defaultMax = defaultMax;
  }

  protected void setDefaultMin(Long defaultMin) {
    this.defaultMin = defaultMin;
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException, UDFInputSeriesDataTypeNotValidException {
    satisfyValueCount = 0L;
    satisfyValueStartTime = 0L;
    satisfyValueLastTime = -1L;
    intervals = new ArrayList<>(0);

    dataType = parameters.getDataType(0);
    min = parameters.getLongOrDefault("min", defaultMin);
    max = parameters.getLongOrDefault("max", defaultMax);
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.INT64);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    switch (dataType) {
      case INT32:
        transformInt(row.getTime(), row.getInt(0));
        break;
      case INT64:
        transformLong(row.getTime(), row.getLong(0));
        break;
      case FLOAT:
        transformFloat(row.getTime(), row.getFloat(0));
        break;
      case DOUBLE:
        transformDouble(row.getTime(), row.getDouble(0));
        break;
      case BOOLEAN:
        transformBoolean(row.getTime(), row.getBoolean(0));
        break;
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  protected void transformDouble(long time, double value) {
    if (satisfyDouble(value) && satisfyValueCount == 0L) {
      satisfyValueCount++;
      satisfyValueStartTime = time;
      satisfyValueLastTime = time;
    } else if (satisfyDouble(value)) {
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else {
      if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
        intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
      }
      satisfyValueCount = 0L;
    }
  }

  protected void transformFloat(long time, float value) {
    if (satisfyFloat(value) && satisfyValueCount == 0L) {
      satisfyValueCount++;
      satisfyValueStartTime = time;
      satisfyValueLastTime = time;
    } else if (satisfyFloat(value)) {
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else {
      if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
        intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
      }
      satisfyValueCount = 0L;
    }
  }

  protected void transformLong(long time, long value) {
    if (satisfyLong(value) && satisfyValueCount == 0L) {
      satisfyValueCount++;
      satisfyValueStartTime = time;
      satisfyValueLastTime = time;
    } else if (satisfyLong(value)) {
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else {
      if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
        intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
      }
      satisfyValueCount = 0L;
    }
  }

  protected void transformInt(long time, int value) {
    if (satisfyInt(value) && satisfyValueCount == 0L) {
      satisfyValueCount++;
      satisfyValueStartTime = time;
      satisfyValueLastTime = time;
    } else if (satisfyInt(value)) {
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else {
      if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
        intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
      }
      satisfyValueCount = 0L;
    }
  }

  protected void transformBoolean(long time, boolean value) {
    if (satisfyBoolean(value) && satisfyValueCount == 0L) {
      satisfyValueCount++;
      satisfyValueStartTime = time;
      satisfyValueLastTime = time;
    } else if (satisfyBoolean(value)) {
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else {
      if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
        intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
      }
      satisfyValueCount = 0L;
    }
  }

  protected abstract Long getRecord();

  @Override
  public void terminate(PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    switch (dataType) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        if (satisfyValueCount != 0) {
          if (getRecord() >= min && getRecord() <= max)
            intervals.add(new Pair<>(satisfyValueStartTime, getRecord()));
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
    for (Pair<Long, Long> pair : intervals) {
      collector.putLong(pair.left, pair.right);
    }
  }

  protected ArrayList<Pair<Long, Long>> intervals;

  protected abstract boolean satisfyInt(int value);

  protected abstract boolean satisfyLong(long value);

  protected abstract boolean satisfyFloat(float value);

  protected abstract boolean satisfyDouble(double value);

  protected abstract boolean satisfyBoolean(Boolean value);
}
