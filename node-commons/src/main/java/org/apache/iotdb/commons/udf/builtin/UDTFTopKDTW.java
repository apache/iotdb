package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UDTFTopKDTW implements UDTF {

  protected static final Logger LOGGER = LoggerFactory.getLogger(UDTFTopKDTW.class);

  private static final int DEFAULT_BATCH_SIZE = 65536;

  private static final String K = "k";
  private static final String BATCH_SIZE = "batchSize";

  protected int k;
  protected int batchSize;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(2).validateRequiredAttribute(K);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    k = parameters.getInt(K);
    if (k <= 0) {
      throw new UDFParameterNotValidException("k must be positive");
    }

    batchSize = parameters.getIntOrDefault(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    if (batchSize <= 0) {
      throw new UDFParameterNotValidException("batchSize must be positive");
    }

    configurations.setAccessStrategy(new SlidingSizeWindowAccessStrategy(batchSize));
  }
}
