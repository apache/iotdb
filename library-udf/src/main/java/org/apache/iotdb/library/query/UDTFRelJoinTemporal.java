package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.type.Type;


import java.util.ArrayList;
import java.util.List;

public class UDTFRelJoinTemporal implements UDTF {
  List<Temporal> temporal_data = new ArrayList<>();

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    String event = parameters.getString("Events");
    String[] temp = event.split(",");
    for (int i = 0; i < temp.length; i++) {
      String[] tmp = temp[i].split("\\|");
      temporal_data.add(
          new Temporal(Long.parseLong(tmp[0]), Long.parseLong(tmp[1]), Integer.parseInt(tmp[2])));
    }
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    for (Temporal rec : temporal_data) {
      if (row.getTime() >= rec.st && row.getTime() < rec.ed) {
        collector.putDouble(row.getTime(), rec.tag);
      }
    }
  }

  private class Temporal {
    long st, ed;
    int tag;

    public Temporal(long st, long ed, int tag) {
      this.st = st;
      this.ed = ed;
      this.tag = tag;
    }
  }
}
