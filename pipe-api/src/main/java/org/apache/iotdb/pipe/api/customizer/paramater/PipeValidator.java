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

package org.apache.iotdb.pipe.api.customizer.paramater;

import org.apache.iotdb.pipe.api.exception.PipeAttributeNotProvidedException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeInputSeriesDataTypeNotValidException;
import org.apache.iotdb.pipe.api.exception.PipeInputSeriesIndexNotValidException;
import org.apache.iotdb.pipe.api.exception.PipeInputSeriesNumberNotValidException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Type;

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
   * @throws PipeInputSeriesDataTypeNotValidException if the data type of the input series at the
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

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index index of the specified column
   * @param expectedDataTypes the expected data types
   * @throws PipeInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   * @throws PipeInputSeriesDataTypeNotValidException if the data type of the input series at the
   *     specified column is not as expected
   */
  public PipeValidator validateInputSeriesDataType(int index, Type... expectedDataTypes)
      throws PipeException {
    validateInputSeriesIndex(index);

    Type actualDataType;
    actualDataType = parameters.getDataType(index);

    for (Type expectedDataType : expectedDataTypes) {
      if (expectedDataType.equals(actualDataType)) {
        return this;
      }
    }

    throw new PipeInputSeriesDataTypeNotValidException(index, actualDataType, expectedDataTypes);
  }

  /**
   * Validates whether the number of the input series is as expected.
   *
   * @param expectedSeriesNumber the expected number of the input series
   * @throws PipeInputSeriesNumberNotValidException if the number of the input series is not as
   *     expected
   */
  public PipeValidator validateInputSeriesNumber(int expectedSeriesNumber)
      throws PipeInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getChildExpressionsSize();
    if (actualSeriesNumber != expectedSeriesNumber) {
      throw new PipeInputSeriesNumberNotValidException(actualSeriesNumber, expectedSeriesNumber);
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
   * @throws PipeInputSeriesNumberNotValidException if the number of the input series is not as
   *     expected
   */
  public PipeValidator validateInputSeriesNumber(
      int expectedSeriesNumberLowerBound, int expectedSeriesNumberUpperBound)
      throws PipeInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getChildExpressionsSize();
    if (actualSeriesNumber < expectedSeriesNumberLowerBound
        || expectedSeriesNumberUpperBound < actualSeriesNumber) {
      throw new PipeInputSeriesNumberNotValidException(
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
   * @throws PipeParameterNotValidException if the given argument is not valid
   */
  public PipeValidator validate(
      PipeValidator.SingleObjectValidationRule validationRule,
      String messageToThrow,
      Object argument)
      throws PipeParameterNotValidException {
    if (!validationRule.validate(argument)) {
      throw new PipeParameterNotValidException(messageToThrow);
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
   * @throws PipeParameterNotValidException if the given arguments are not valid
   */
  public PipeValidator validate(
      PipeValidator.MultipleObjectsValidationRule validationRule,
      String messageToThrow,
      Object... arguments)
      throws PipeParameterNotValidException {
    if (!validationRule.validate(arguments)) {
      throw new PipeParameterNotValidException(messageToThrow);
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
   * @throws PipeInputSeriesIndexNotValidException if the index of the specified column is out of
   *     bound
   */
  private void validateInputSeriesIndex(int index) throws PipeInputSeriesIndexNotValidException {
    int actualSeriesNumber = parameters.getChildExpressionsSize();
    if (index < 0 || actualSeriesNumber <= index) {
      throw new PipeInputSeriesIndexNotValidException(index, actualSeriesNumber);
    }
  }
}
