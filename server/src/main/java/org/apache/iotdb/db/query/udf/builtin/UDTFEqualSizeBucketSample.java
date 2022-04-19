package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFEqualSizeBucketSample implements UDTF {

  protected TSDataType dataType;
  protected String aggMethodType;
  protected double proportion;
  protected int bucketSize;

  @Override
  public void validate(UDFParameterValidator validator) throws MetadataException, UDFException {
    dataType = validator.getParameters().getDataType(0);
    proportion = validator.getParameters().getDoubleOrDefault("proportion", 0.1);
    aggMethodType = validator.getParameters().getStringOrDefault("type", "avg").toLowerCase();
    bucketSize = (int) (1 / proportion);
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            proportion -> (double) proportion > 0 && (double) proportion <= 1,
            "Illegal sample proportion.",
            proportion)
        .validate(
            type ->
                "avg".equals(type)
                    || "max".equals(type)
                    || "min".equals(type)
                    || "sum".equals(type)
                    || "extreme".equals(type)
                    || "variance".equals(type),
            "Illegal aggregation method.",
            aggMethodType);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {}
}
