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

package org.apache.iotdb.commons.exception;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

public class ConfigurationException extends IoTDBException {
  private String parameter;
  private String correctValue;

  /**
   * Notice: The error reason should always be set for ease of use.
   *
   * @param parameter The error parameter
   * @param badValue The bad value
   * @param correctValue The correct value (if it could be listed)
   * @param reason The reason why lead to the exception
   */
  public ConfigurationException(
      String parameter, String badValue, String correctValue, String reason) {
    super(
        String.format(
            "Parameter %s can not be %s, please set to: %s. Because %s",
            parameter, badValue, correctValue, reason),
        TSStatusCode.CONFIGURATION_ERROR.getStatusCode());
    this.parameter = parameter;
    this.correctValue = correctValue;
  }

  /** Notice: The error reason should always be set for ease of use. */
  public ConfigurationException(String reason) {
    super(reason, TSStatusCode.CONFIGURATION_ERROR.getStatusCode());
  }

  @TestOnly
  public String getParameter() {
    return parameter;
  }

  @TestOnly
  public String getCorrectValue() {
    return correctValue;
  }
}
