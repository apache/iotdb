package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.anomaly.util.MasterDetector;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.Objects;

public class UDTFMasterDetect implements UDTF {

  private MasterDetector masterDetector;
  private int output_column;
  private String output_type;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }
    if (validator.getParameters().hasAttribute("k")) {
      validator.validate(
          k -> (int) k > 0,
          "Parameter k should be a positive integer.",
          validator.getParameters().getInt("k"));
    }
    if (validator.getParameters().hasAttribute("p")) {
      validator.validate(
          p -> (int) p > 0,
          "Order p should be a positive integer.",
          validator.getParameters().getInt("p"));
    }
    if (validator.getParameters().hasAttribute("output_column")) {
      validator.validate(
          output_column -> (int) output_column > 0,
          "Parameter output_column should be a positive integer.",
          validator.getParameters().getInt("output_column"));
    }
    if (validator.getParameters().hasAttribute("eta")) {
      validator.validate(
          eta -> (double) eta > 0,
          "Parameter eta should be larger than 0.",
          validator.getParameters().getDouble("eta"));
    }
    if (validator.getParameters().hasAttribute("beta")) {
      validator.validate(
          beta -> (double) beta > 0,
          "Parameter beta should be larger than 0.",
          validator.getParameters().getDouble("beta"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    output_type = parameters.getStringOrDefault("output_type", "repair");
    if (output_type.equals("repairing")) configurations.setOutputDataType(Type.DOUBLE);
    else configurations.setOutputDataType(Type.BOOLEAN);
    int columnCnt = (parameters.getDataTypes().size() - 1) / 2;
    int k = parameters.getIntOrDefault("k", -1);
    int p = parameters.getIntOrDefault("p", -1);
    double eta = parameters.getDoubleOrDefault("eta", 1.0);
    double beta = parameters.getDoubleOrDefault("beta", 1.0);
    output_column = parameters.getIntOrDefault("output_column", 1);
    masterDetector = new MasterDetector(columnCnt, k, p, eta, beta);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!masterDetector.isNullRow(row)) {
      masterDetector.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    masterDetector.detectAndRepair();
    ArrayList<ArrayList<Double>> td_repaired = masterDetector.getTd_repaired();
    ArrayList<Long> td_time = masterDetector.getTd_time();
    ArrayList<Boolean> anomalies_in_repaired = masterDetector.getAnomalies_in_repaired();
    if (Objects.equals(output_type, "repair")) {
      for (int i = 0; i < td_repaired.size(); i++) {
        collector.putDouble(td_time.get(i), td_repaired.get(i).get(output_column));
      }
    } else {
      for (int i = 0; i < anomalies_in_repaired.size(); i++) {
        collector.putBoolean(td_time.get(i), anomalies_in_repaired.get(i));
      }
    }
  }
}
