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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TypeInferenceUtilsTest {

  @Test
  public void isNumberTest() {
    String[] values = {"123", "123.123", "-123.123", "+123.123", ".123", String.valueOf(Integer.MAX_VALUE),
        String.valueOf(Integer.MIN_VALUE), "abc", "123.", "123a", ".", "123.1.23"};
    boolean[] results = {true, true, true, true, true, true,
        true, false, false, false, false, false};

    for (int i = 0; i < values.length; i++) {
      assertEquals(TypeInferenceUtils.isNumber(values[i]), results[i]);
    }
  }

  @Test
  public void getPredictedDataTypeTest() {
    Object[] values = {123, "abc", 123.123, true};
    TSDataType[] encodings = {TSDataType.INT64, TSDataType.TEXT, TSDataType.DOUBLE, TSDataType.BOOLEAN};

    for (int i = 0; i < values.length; i++) {
      assertEquals(TypeInferenceUtils.getPredictedDataType(values[i]), encodings[i]);
    }
  }
}
