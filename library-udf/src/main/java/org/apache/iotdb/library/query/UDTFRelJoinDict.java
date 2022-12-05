package org.apache.iotdb.library.query;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

public class UDTFRelJoinDict implements UDTF {
  Map<Integer, Double> dic = new HashMap<>();

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    String[] dict = parameters.getString("dict").split(":");
    for (int i = 0; i < dict.length; i++) {
      String[] app = dict[i].split("\\|");
      if (!dic.containsKey(Integer.parseInt(app[0]))) {
        dic.put(Integer.parseInt(app[0]), Double.parseDouble(app[1]));
      }
    }
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Integer key = (int) row.getDouble(0) * 1000;
    if (dic.containsKey(key)) {
      collector.putDouble(row.getTime(), dic.get(key));
    } // else collector.putDouble(row.getTime(), key);
  }
}
