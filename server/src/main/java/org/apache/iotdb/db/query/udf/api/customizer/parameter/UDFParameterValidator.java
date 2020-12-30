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
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesIndexNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesNumberNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDFParameterValidator {

  private final String functionName;
  private final UDFParameters parameters;

  public UDFParameterValidator(String functionName, UDFParameters parameters) {
    this.functionName = functionName;
    this.parameters = parameters;
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
      throw new UDFAttributeNotProvidedException(functionName, attributeKey);
    }
    return this;
  }

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index            index of the specified column
   * @param expectedDataType the expected data type
   * @throws UDFInputSeriesIndexNotValidException    if the index of the specified column is out of
   *                                                 bound
   * @throws UDFInputSeriesDataTypeNotValidException if the data type of the input series at the
   *                                                 specified column is not as expected
   * @throws MetadataException                       if error occurs when getting the data type of
   *                                                 the input series
   */
  public UDFParameterValidator validateInputSeriesDataType(int index, TSDataType expectedDataType)
      throws UDFInputSeriesIndexNotValidException, UDFInputSeriesDataTypeNotValidException, MetadataException {
    validateInputSeriesIndex(index);

    TSDataType actualDataType = parameters.getDataType(index);
    if (!expectedDataType.equals(actualDataType)) {
      throw new UDFInputSeriesDataTypeNotValidException(functionName, index, actualDataType,
          expectedDataType);
    }
    return this;
  }

  /**
   * Validates whether the data type of the input series at the specified column is as expected.
   *
   * @param index             index of the specified column
   * @param expectedDataTypes the expected data types
   * @throws UDFInputSeriesIndexNotValidException    if the index of the specified column is out of
   *                                                 bound
   * @throws UDFInputSeriesDataTypeNotValidException if the data type of the input series at the
   *                                                 specified column is not as expected
   * @throws MetadataException                       if error occurs when getting the data type of
   *                                                 the input series
   */
  public UDFParameterValidator validateInputSeriesDataType(int index,
      TSDataType... expectedDataTypes)
      throws UDFInputSeriesIndexNotValidException, UDFInputSeriesDataTypeNotValidException, MetadataException {
    validateInputSeriesIndex(index);

    TSDataType actualDataType = parameters.getDataType(index);
    for (TSDataType expectedDataType : expectedDataTypes) {
      if (expectedDataType.equals(actualDataType)) {
        return this;
      }
    }

    throw new UDFInputSeriesDataTypeNotValidException(functionName, index, actualDataType,
        expectedDataTypes);
  }

  /**
   * Validates whether the number of the input series is as expected.
   *
   * @param expectedSeriesNumber the expected number of the input series
   * @throws UDFInputSeriesNumberNotValidException if the number of the input series is not as
   *                                               expected
   */
  public UDFParameterValidator validateInputSeriesNumber(int expectedSeriesNumber)
      throws UDFInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (actualSeriesNumber != expectedSeriesNumber) {
      throw new UDFInputSeriesNumberNotValidException(functionName, actualSeriesNumber,
          expectedSeriesNumber);
    }
    return this;
  }

  /**
   * Validates whether the number of the input series is as expected.
   *
   * @param expectedSeriesNumberLowerBound the number of the input series must be greater than or
   *                                       equal to the expectedSeriesNumberLowerBound
   * @param expectedSeriesNumberUpperBound the number of the input series must be less than or equal
   *                                       to the expectedSeriesNumberUpperBound
   * @throws UDFInputSeriesNumberNotValidException if the number of the input series is not as
   *                                               expected
   */
  public UDFParameterValidator validateInputSeriesNumber(int expectedSeriesNumberLowerBound,
      int expectedSeriesNumberUpperBound) throws UDFInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (actualSeriesNumber < expectedSeriesNumberLowerBound
        || expectedSeriesNumberUpperBound < actualSeriesNumber) {
      throw new UDFInputSeriesNumberNotValidException(functionName, actualSeriesNumber,
          expectedSeriesNumberLowerBound, expectedSeriesNumberUpperBound);
    }
    return this;
  }

  /**
   * Validates whether the index of the specified column is out of bound.
   * </p>
   * bound: [0, parameters.getPaths().size())
   *
   * @param index the index of the specified column
   * @throws UDFInputSeriesIndexNotValidException if the index of the specified column is out of
   *                                              bound
   */
  public UDFParameterValidator validateInputSeriesIndex(int index)
      throws UDFInputSeriesIndexNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (index < 0 || actualSeriesNumber <= index) {
      throw new UDFInputSeriesIndexNotValidException(functionName, index, actualSeriesNumber);
    }
    return this;
  }
}
