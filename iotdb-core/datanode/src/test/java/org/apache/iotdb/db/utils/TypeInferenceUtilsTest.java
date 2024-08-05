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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TypeInferenceUtilsTest {

  @Test
  public void isNumberTest() {
    String[] values = {
      "123",
      "123.123",
      "-123.123",
      "+123.123",
      ".123",
      String.valueOf(Integer.MAX_VALUE),
      String.valueOf(Integer.MIN_VALUE),
      String.valueOf(Double.MAX_VALUE),
      String.valueOf(Double.MIN_VALUE),
      "abc",
      "123.",
      "123a",
      TsFileConstant.PATH_SEPARATOR,
      "123.1.23",
      "77123 ",
      " 7112324 "
    };
    boolean[] results = {
      true, true, true, true, true, true, true, true, true, false, true, false, false, false, true,
      true
    };

    for (int i = 0; i < values.length; i++) {
      assertEquals(TypeInferenceUtils.isNumber(values[i]), results[i]);
    }
  }

  @Test
  public void testInferType() throws QueryProcessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    Object[] values = {
      123,
      "abc",
      123.123d,
      true,
      123.1f,
      "123",
      "12.2",
      "9999999999999999",
      "true",
      "77123 ",
      " 7112324 ",
      "16777217", // 2^24 + 1
      "16777216", // 2^24
      "271840880000000000000000",
      "4.9387406015404442E17",
      "4E5",
      "1.0",
      "4F",
      "4L"
    };
    TSDataType[] inferredTypes = {
      TSDataType.INT32,
      TSDataType.TEXT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN,
      TSDataType.FLOAT,
      config.getIntegerStringInferType(),
      config.getFloatingStringInferType(),
      config.getIntegerStringInferType(),
      config.getBooleanStringInferType(),
      config.getIntegerStringInferType(),
      config.getIntegerStringInferType(),
      config.getIntegerStringInferType(),
      config.getIntegerStringInferType(),
      config.getFloatingStringInferType(),
      config.getFloatingStringInferType(),
      config.getFloatingStringInferType(),
      config.getFloatingStringInferType(),
      TSDataType.TEXT,
      TSDataType.TEXT,
    };

    for (int i = 0; i < values.length; i++) {
      assertEquals(inferredTypes[i], TypeInferenceUtils.getPredictedDataType(values[i], true));
    }
  }

  @Test
  public void testNotInferType() {
    Object[] values = {123, "abc", 123.123d, true, 123.1f, "123", "12.2", "true"};
    TSDataType[] encodings = {
      TSDataType.INT32,
      TSDataType.TEXT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN,
      TSDataType.FLOAT,
      TSDataType.TEXT,
      TSDataType.TEXT,
      TSDataType.TEXT
    };

    for (int i = 0; i < values.length; i++) {
      assertEquals(encodings[i], TypeInferenceUtils.getPredictedDataType(values[i], false));
    }
  }
}
