package org.apache.iotdb.pipe.api.customizer.paramater;

import org.apache.iotdb.pipe.api.exception.PipeAttributeNotProvidedException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

public class PipeValidator {

  private final PipeParameters parameters;

  public PipeValidator(PipeParameters parameters) {
    this.parameters = parameters;
  }

  public PipeParameters getParameters() {
    return parameters;
  }

  /**
   * Validates whether the attributes entered by the user contain an attribute whose key is
   * attributeKey.
   *
   * @param key key of the attribute
   * @throws PipeAttributeNotProvidedException if the attribute is not provided
   */
  public PipeValidator validateRequiredAttribute(String key)
      throws PipeAttributeNotProvidedException {
    if (!parameters.hasAttribute(key)) {
      throw new PipeAttributeNotProvidedException(key);
    }
    return this;
  }

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index index of the specified column
   * @param expectedDataType the expected data type
   * @throws PipeInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   * @throws UDFInputSeriesDataTypeNotValidException if the data type of the input series at the
   *     specified column is not as expected
   */
  public PipeValidator validateInputSeriesDataType(int index, Type expectedDataType)
      throws PipeException {
    validateInputSeriesIndex(index);

    Type actualDataType;
    actualDataType = parameters.getDataType(index);

    if (!expectedDataType.equals(actualDataType)) {
      throw new PipeInputSeriesDataTypeNotValidException(index, actualDataType, expectedDataType);
    }
    return this;
  }
}
