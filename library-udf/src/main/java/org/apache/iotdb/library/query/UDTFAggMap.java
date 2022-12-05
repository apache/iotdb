package org.apache.iotdb.library.query;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class UDTFAggMap implements UDTF {
  private String func;
  long st, ed;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    func = parameters.getString("func");
    st = parameters.getLong("start");
    ed = parameters.getLong("end");
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) { // skip null rows
      return;
    }
    if (row.getTime() > ed || row.getTime() < st) { // skip null rows
      return;
    }
    double res;
    switch (func) {
      case "min":
        res = findAvg(row);
        break;
      case "spe":
        res = findSpecial(row);
        break;
      default:
        res = findMax(row);
    }
    collector.putDouble(row.getTime(), res);
  }

  public double findMax(Row row) throws IOException {
    double ans = Double.MIN_VALUE;
    for (int i = 0; i < row.size(); i++) {
      ans = Double.max(ans, row.getDouble(i));
    }
    return ans;
  }

  public double findAvg(Row row) throws IOException {
    double ans = 0.;
    for (int i = 0; i < row.size(); i++) {
      ans += row.getDouble(i);
    }
    return ans / row.size();
  }

  public double findSpecial(Row row) throws IOException {
    double ans = 0.;
    for (int i = 0; i < row.size(); i++) {
      ans += row.getDouble(i) * row.getDouble(i);
    }
    return ans;
  }
}
