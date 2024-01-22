package org.apache.iotdb.db.queryengine.transformation.dag.udf;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class UDAFInformationInferrer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDAFInformationInferrer.class);

  protected final String functionName;

  public UDAFInformationInferrer(String functionName) {
    this.functionName = functionName;
  }

  public TSDataType inferOutputType(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    try {
      return UDFDataTypeTransformer.transformToTsDataType(
          reflectAndGetConfigurations(childExpressions, childExpressionDataTypes, attributes)
              .getOutputDataType());
    } catch (Exception e) {
      LOGGER.warn("Error occurred during inferring UDF data type", e);
      throw new SemanticException(
          String.format("Error occurred during inferring UDF data type: %s", System.lineSeparator())
              + e);
    }
  }

  private UDAFConfigurations reflectAndGetConfigurations(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes)
      throws Exception {
    UDAF udaf = (UDAF) UDFManagementService.getInstance().reflect(functionName);

    UDFParameters parameters =
        new UDFParameters(
            childExpressions,
            UDFDataTypeTransformer.transformToUDFDataTypeList(childExpressionDataTypes),
            attributes);
    udaf.validate(new UDFParameterValidator(parameters));

    // use ZoneId.systemDefault() because UDF's data type is ZoneId independent
    UDAFConfigurations configurations = new UDAFConfigurations(ZoneId.systemDefault());
    udaf.beforeStart(parameters, configurations);
    udaf.beforeDestroy();
    return configurations;
  }
}
