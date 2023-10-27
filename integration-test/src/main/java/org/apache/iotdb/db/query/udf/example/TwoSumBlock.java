package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.tsfile.access.Column;
import org.apache.iotdb.tsfile.access.ColumnBuilder;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

public class TwoSumBlock implements UDTF {
  private Type type;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateInputSeriesDataType(1, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    type = parameters.getDataType(0);
    configurations.setAccessStrategy(new MappableRowByRowAccessStrategy()).setOutputDataType(type);
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    switch (type) {
      case INT32:
        transformInt(columns, builder);
        return;
      case INT64:
        transformLong(columns, builder);
        return;
      case FLOAT:
        transformFloat(columns, builder);
        return;
      case DOUBLE:
        transformDouble(columns, builder);
        return;
      default:
        throw new Exception();
    }
  }

  public void transformInt(Column[] columns, ColumnBuilder builder) throws Exception {
    int[] inputs1 = columns[0].getInts();
    int[] inputs2 = columns[1].getInts();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      builder.writeInt(inputs1[i] + inputs2[i]);
    }
  }

  public void transformLong(Column[] columns, ColumnBuilder builder) throws Exception {
    long[] inputs1 = columns[0].getLongs();
    long[] inputs2 = columns[1].getLongs();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      builder.writeLong(inputs1[i] + inputs2[i]);
    }
  }

  public void transformFloat(Column[] columns, ColumnBuilder builder) throws Exception {
    float[] inputs1 = columns[0].getFloats();
    float[] inputs2 = columns[1].getFloats();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      builder.writeFloat(inputs1[i] + inputs2[i]);
    }
  }

  public void transformDouble(Column[] columns, ColumnBuilder builder) throws Exception {
    double[] inputs1 = columns[0].getDoubles();
    double[] inputs2 = columns[1].getDoubles();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      builder.writeDouble(inputs1[i] + inputs2[i]);
    }
  }
}
