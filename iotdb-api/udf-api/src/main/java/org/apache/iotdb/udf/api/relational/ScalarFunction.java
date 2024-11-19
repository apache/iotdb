package org.apache.iotdb.udf.api.relational;

import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.relational.data.Record;
import org.apache.iotdb.udf.api.type.Type;

public interface ScalarFunction extends SQLFunction {

  /**
   * This method is mainly used to validate {@link FunctionParameters} and infer output data type.
   *
   * @param parameters parameters used to validate
   * @throws Exception if any parameter is not valid
   */
  Type validateAndInferOutputType(FunctionParameters parameters) throws Exception;

  /**
   * This method will be called to process the transformation. In a single UDF query, this method
   * may be called multiple times.
   *
   * @param input original input data row
   * @throws Exception the user can throw errors if necessary
   * @throws UnsupportedOperationException if the user does not override this method
   * @see MappableRowByRowAccessStrategy
   */
  Object evaluate(Record input) throws Exception;

  /** This method is mainly used to release the resources used in the SQLFunction. */
  default void beforeDestroy() {
    // do nothing
  }
}
