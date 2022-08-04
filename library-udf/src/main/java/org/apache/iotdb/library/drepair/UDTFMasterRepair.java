package org.apache.iotdb.library.drepair;

import org.apache.iotdb.library.drepair.util.MasterRepairUtil;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

public class UDTFMasterRepair implements UDTF {
  private MasterRepairUtil masterRepairUtil;
  private int output_column;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }
    if (validator.getParameters().hasAttribute("omega")) {
      validator.validate(
          omega -> (int) omega >= 0,
          "Parameter omega should be non-negative.",
          validator.getParameters().getInt("omega"));
    }
    if (validator.getParameters().hasAttribute("eta")) {
      validator.validate(
          eta -> (double) eta > 0,
          "Parameter eta should be larger than 0.",
          validator.getParameters().getDouble("eta"));
    }
    if (validator.getParameters().hasAttribute("k")) {
      validator.validate(
          k -> (int) k > 0,
          "Parameter k should be a positive integer.",
          validator.getParameters().getInt("k"));
    }
    if (validator.getParameters().hasAttribute("output_column")) {
      validator.validate(
          output_column -> (int) output_column > 0,
          "Parameter output_column should be a positive integer.",
          validator.getParameters().getInt("output_column"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    configurations.setOutputDataType(Type.DOUBLE);
    int columnCnt = parameters.getDataTypes().size() / 2;
    long omega = parameters.getLongOrDefault("omega", -1);
    double eta = parameters.getDoubleOrDefault("eta", Double.NaN);
    int k = parameters.getIntOrDefault("k", -1);
    masterRepairUtil = new MasterRepairUtil(columnCnt, omega, eta, k);
    output_column = parameters.getIntOrDefault("output_column", 1);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!masterRepairUtil.isNullRow(row)) {
      masterRepairUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    masterRepairUtil.repair();
    ArrayList<Long> times = masterRepairUtil.getTime();
    ArrayList<Double> column = masterRepairUtil.getCleanResultColumn(this.output_column);
    for (int i = 0; i < column.size(); i++) {
      collector.putDouble(times.get(i), column.get(i));
    }
  }
}
