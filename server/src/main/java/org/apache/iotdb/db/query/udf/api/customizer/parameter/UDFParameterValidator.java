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

  public UDFParameterValidator validateRequiredAttribute(String attributeKey)
      throws UDFAttributeNotProvidedException {
    if (!parameters.hasAttribute(attributeKey)) {
      throw new UDFAttributeNotProvidedException(functionName, attributeKey);
    }
    return this;
  }

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

  public UDFParameterValidator validateInputSeriesNumber(int expectedSeriesNumber)
      throws UDFInputSeriesNumberNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (actualSeriesNumber != expectedSeriesNumber) {
      throw new UDFInputSeriesNumberNotValidException(functionName, actualSeriesNumber,
          expectedSeriesNumber);
    }
    return this;
  }

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

  public UDFParameterValidator validateInputSeriesIndex(int index)
      throws UDFInputSeriesIndexNotValidException {
    int actualSeriesNumber = parameters.getPaths().size();
    if (index < 0 || actualSeriesNumber <= index) {
      throw new UDFInputSeriesIndexNotValidException(functionName, index, actualSeriesNumber);
    }
    return this;
  }
}
