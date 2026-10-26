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

package org.apache.iotdb.udf.api.exception;

import org.apache.iotdb.udf.api.i18n.UdfApiMessages;

public class UDFInputSeriesNumberNotValidException extends UDFParameterNotValidException {

  public UDFInputSeriesNumberNotValidException(int actual, int expected) {
    super(
        String.format(
            UdfApiMessages
                .EXCEPTION_THE_NUMBER_OF_THE_INPUT_SERIES_IS_NOT_VALID_EXPECTED_ARG_ACTUAL_ARG_48AF79C9,
            expected,
            actual));
  }

  public UDFInputSeriesNumberNotValidException(
      int actual, int expectedLowerBound, int expectedUpperBound) {
    super(
        String.format(
            UdfApiMessages
                .EXCEPTION_THE_NUMBER_OF_THE_INPUT_SERIES_IS_NOT_VALID_EXPECTED_ARG_ARG_ACTUAL_ARG_819C0F0A,
            expectedLowerBound,
            expectedUpperBound,
            actual));
  }
}
