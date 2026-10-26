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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.commons.i18n.SchemaMessages;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.UnknownType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class UDFDataTypeTransformerTest {

  @Test
  public void testSupportedTypesUseTypeInterfaces() {
    for (Type udfType : Type.allTypes()) {
      TSDataType tsDataType = TSDataType.getTsDataType(udfType.getType());

      Assert.assertEquals(udfType, UDFDataTypeTransformer.transformToUDFDataType(tsDataType));
      org.apache.tsfile.read.common.type.Type readType =
          UDFDataTypeTransformer.transformUDFDataTypeToReadType(udfType);
      Assert.assertEquals(tsDataType.name(), readType.getTypeEnum().name());
      Assert.assertEquals(udfType, UDFDataTypeTransformer.transformReadTypeToUDFDataType(readType));
    }
  }

  @Test
  public void testNullTypesRemainNull() {
    Assert.assertNull(UDFDataTypeTransformer.transformToUDFDataType(null));
    Assert.assertNull(UDFDataTypeTransformer.transformReadTypeToUDFDataType(null));
    Assert.assertNull(UDFDataTypeTransformer.transformUDFDataTypeToReadType(null));
  }

  @Test
  public void testInternalTypesRemainUnsupported() {
    assertInvalidInput(() -> UDFDataTypeTransformer.transformToUDFDataType(TSDataType.UNKNOWN));
    assertInvalidInput(() -> UDFDataTypeTransformer.transformToUDFDataType(TSDataType.VECTOR));
    assertInvalidInput(
        () -> UDFDataTypeTransformer.transformReadTypeToUDFDataType(UnknownType.UNKNOWN));
    assertInvalidInput(
        () ->
            UDFDataTypeTransformer.transformReadTypeToUDFDataType(
                RowType.anonymous(Collections.emptyList())));
  }

  private static void assertInvalidInput(Runnable conversion) {
    IllegalArgumentException exception =
        Assert.assertThrows(IllegalArgumentException.class, conversion::run);
    Assert.assertTrue(exception.getMessage().startsWith(SchemaMessages.SCHEMA_INVALID_INPUT));
  }
}
