/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.udf.api.customizer.parameter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.exception.UDFAttributeNotProvidedException;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesIndexNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesNumberNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDFParameterValidator {

  private final UDFParameters parameters;

  public UDFParameterValidator(UDFParameters parameters) {
    this.parameters = parameters;
  }

  public UDFParameters getParameters() {
    return parameters;
  }

  /**
   * Validates whether the attributes entered by the user contain an attribute whose key is
   * attributeKey.
   *
   * @param attributeKey key of the attribute
   * @throws UDFAttributeNotProvidedException if the attribute is not provided
   */
  public UDFParameterValidator validateRequiredAttribute(String attributeKey)
      throws UDFAttributeNotProvidedException {
    if (!parameters.hasAttribute(attributeKey)) {
      throw new UDFAttributeNotProvidedException(attributeKey);
    }
    return this;
  }

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index index of the specified column
   * @param expectedDataType the expected data type
   * @throws UDFInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   * @throws UDFInputSeriesDataTypeNotValidException if the data type of the input series at the
   *     specified column is not as expected
   */
  public UDFParameterValidator validateInputSeriesDataType(int index, TSDataType expectedDataType)
      throws UDFException {
    validateInputSeriesIndex(index);

    TSDataType actualDataType;
    try {
      actualDataType = parameters.getDataType(index);
    } catch (MetadataException e) {
      throw new UDFException("error occurred when getting the data type of the input series");
    }

    if (!expectedDataType.equals(actualDataType)) {
      throw new UDFInputSeriesDataTypeNotValidException(index, actualDataType, expectedDataType);
    }
    return this;
  }

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index index of the specified column
   * @param expectedDataTypes the expected data types
   * @throws UDFInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   * @throws UDFInputSeriesDataTypeNotValidException if the data type of the input series at the
   *     specified column is not as expected
   */
  public UDFParameterValidator validateInputSeriesDataType(
      int index, TSDataType... expectedDataTypes) throws UDFException {
    validateInputSeriesIndex(index);

    TSDataType actualDataType;
    try {
      actualDataType = parameters.getDataType(index);
    } catch (MetadataException e) {
      throw new UDFException("error occurred when getting the data type of the input series");
    }

    for (TSDataType expectedDataType : expectedDataTypes) {
      if (expectedDataType.equals(actualDataType)) {
        return this;
      }
    }

    throw new UDFInputSeriesDataTypeNotValidException(index, actualDataType, expectedDataTypes);
  }

  /**
   * Validates whether the number of the input series is as expected.
   *
   * @param expectedSeriesNumber the expected number of the input series
   * @throws UDFInputSeriesNumberNotValidException if the number of the input series is not as
   *     expected
   */
  public UDFParameterValidator validateInputSeriesNumber(int expectedSeriesNumber)
      throws UDFInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (actualSeriesNumber != expectedSeriesNumber) {
      throw new UDFInputSeriesNumberNotValidException(actualSeriesNumber, expectedSeriesNumber);
    }
    return this;
  }

  /**
   * Validates whether the number of the input series is as expected.
   *
   * @param expectedSeriesNumberLowerBound the number of the input series must be greater than or
   *     equal to the expectedSeriesNumberLowerBound
   * @param expectedSeriesNumberUpperBound the number of the input series must be less than or equal
   *     to the expectedSeriesNumberUpperBound
   * @throws UDFInputSeriesNumberNotValidException if the number of the input series is not as
   *     expected
   */
  public UDFParameterValidator validateInputSeriesNumber(
      int expectedSeriesNumberLowerBound, int expectedSeriesNumberUpperBound)
      throws UDFInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (actualSeriesNumber < expectedSeriesNumberLowerBound
        || expectedSeriesNumberUpperBound < actualSeriesNumber) {
      throw new UDFInputSeriesNumberNotValidException(
          actualSeriesNumber, expectedSeriesNumberLowerBound, expectedSeriesNumberUpperBound);
    }
    return this;
  }

  /**
   * Validates the input parameters according to the validation rule given by the user.
   *
   * @param validationRule the validation rule, which can be a lambda expression
   * @param messageToThrow the message to throw when the given argument is not valid
   * @param argument the given argument
   * @throws UDFParameterNotValidException if the given argument is not valid
   */
  public UDFParameterValidator validate(
      SingleObjectValidationRule validationRule, String messageToThrow, Object argument)
      throws UDFParameterNotValidException {
    if (!validationRule.validate(argument)) {
      throw new UDFParameterNotValidException(messageToThrow);
    }
    return this;
  }

  public interface SingleObjectValidationRule {

    boolean validate(Object arg);
  }

  /**
   * Validates the input parameters according to the validation rule given by the user.
   *
   * @param validationRule the validation rule, which can be a lambda expression
   * @param messageToThrow the message to throw when the given arguments are not valid
   * @param arguments the given arguments
   * @throws UDFParameterNotValidException if the given arguments are not valid
   */
  public UDFParameterValidator validate(
      MultipleObjectsValidationRule validationRule, String messageToThrow, Object... arguments)
      throws UDFParameterNotValidException {
    if (!validationRule.validate(arguments)) {
      throw new UDFParameterNotValidException(messageToThrow);
    }
    return this;
  }

  public interface MultipleObjectsValidationRule {

    boolean validate(Object... args);
  }

  /**
   * Validates whether the index of the specified column is out of bound. bound: [0,
   * parameters.getPaths().size())
   *
   * @param index the index of the specified column
   * @throws UDFInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   */
  private void validateInputSeriesIndex(int index) throws UDFInputSeriesIndexNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (index < 0 || actualSeriesNumber <= index) {
      throw new UDFInputSeriesIndexNotValidException(index, actualSeriesNumber);
    }
  }
}
