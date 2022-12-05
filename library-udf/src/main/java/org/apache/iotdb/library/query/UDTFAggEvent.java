package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.type.Type;

import java.util.*;

public class UDTFAggEvent implements UDTF {

  List<Temporal> temporal_data = new ArrayList<>();
  Map<Integer, Double> res = new HashMap<>();
  Map<Integer, Integer> cnt = new HashMap<>();

  String func;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    String event = parameters.getString("Events");
    func = parameters.getString("func");
    String[] temp = event.split(",");
    for (int i = 0; i < temp.length; i++) {
      String[] tmp = temp[i].split("\\|");
      temporal_data.add(
          new Temporal(Long.parseLong(tmp[0]), Long.parseLong(tmp[1]), Integer.parseInt(tmp[2])));
    }
    temporal_data.sort(
        new Comparator<Temporal>() {
          @Override
          public int compare(Temporal o1, Temporal o2) {
            if (o1.st < o2.st) return -1;
            else return 1;
          }
        });
    configurations
        .setAccessStrategy(
            new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE, Integer.MAX_VALUE))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    for (int i = 0; i < rowWindow.windowSize(); i++) {
      Row curr = rowWindow.getRow(i);
      for (int j = 0; j < temporal_data.size(); j++) {
        if (curr.getTime() >= temporal_data.get(j).st
            && curr.getTime() >= temporal_data.get(j).st) {
          int tag = temporal_data.get(j).tag;
          if (func.equals("avg")) {
            if (!cnt.containsKey(tag)) cnt.put(tag, 0);
            cnt.replace(tag, cnt.get(tag) + 1);
          }
          if (func.equals("avg") || func.equals("sum")) {
            if (!res.containsKey(temporal_data.get(j).tag)) {
              res.put(temporal_data.get(j).tag, 0.);
            }
            res.replace(
                temporal_data.get(j).tag, curr.getDouble(0) + res.get(temporal_data.get(j).tag));
          }
        }
      }
    }
    if (func.equals("avg")) {
      for (int k : res.keySet()) {
        res.replace(k, res.get(k) / cnt.get(k));
      }
    }
    for (int k : res.keySet()) {
      collector.putDouble(k, res.get(k));
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
