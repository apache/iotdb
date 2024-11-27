package org.apache.iotdb.db.query.udf.example.relational;

import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

public class ContainNull implements ScalarFunction {
  @Override
  public void validate(FunctionParameters parameters) throws Exception {
    if (parameters.getChildExpressionsSize() < 1) {
      throw new UDFParameterNotValidException("At least one parameter is required.");
    }
  }

  @Override
  public Type inferOutputType(FunctionParameters parameters) {
    return Type.BOOLEAN;
  }

  @Override
  public Object evaluate(Record input) throws Exception {
    for (int i = 0; i < input.size(); i++) {
      if (input.isNull(i)) {
        return true;
      }
    }
    return false;
  }
}
