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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

public class EncodingInferenceUtilsTest {
  @Test
  public void getDefaultEncodingTest() {
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    Assert.assertEquals(
        conf.getDefaultBooleanEncoding(),
        EncodingInferenceUtils.getDefaultEncoding(TSDataType.BOOLEAN));
    Assert.assertEquals(
        conf.getDefaultInt32Encoding(),
        EncodingInferenceUtils.getDefaultEncoding(TSDataType.INT32));
    Assert.assertEquals(
        conf.getDefaultInt64Encoding(),
        EncodingInferenceUtils.getDefaultEncoding(TSDataType.INT64));
    Assert.assertEquals(
        conf.getDefaultFloatEncoding(),
        EncodingInferenceUtils.getDefaultEncoding(TSDataType.FLOAT));
    Assert.assertEquals(
        conf.getDefaultDoubleEncoding(),
        EncodingInferenceUtils.getDefaultEncoding(TSDataType.DOUBLE));
    Assert.assertEquals(
        conf.getDefaultTextEncoding(), EncodingInferenceUtils.getDefaultEncoding(TSDataType.TEXT));
  }
}
