package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** Find subsequence of time-series data satisfying V < b || V > a, a > b. */
public class UDTFOpenValue implements UDTF {
  private double lessThan, greaterThan;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    lessThan = parameters.getDouble("less_than");
    greaterThan = parameters.getDouble("greater_than");
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) { // skip null rows
      return;
    }
    if (row.getDouble(0) <= lessThan || row.getDouble(0) >= greaterThan) {
      collector.putDouble(row.getTime(), row.getDouble(0));
    }
  }
}
