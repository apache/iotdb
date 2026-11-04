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

package org.apache.iotdb.db.queryengine.transformation.dag.input;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ConstantInputReaderTest {

  @Test
  public void testSupportedConstantColumns() throws Exception {
    final TSDataType[] dataTypes = {
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.TEXT,
      TSDataType.BOOLEAN
    };
    final String[] valueStrings = {"1", "2", "3.5", "4.5", "text", "true"};
    final Object[] expectedValues = {
      1, 2L, 3.5F, 4.5D, new Binary("text", TSFileConfig.STRING_CHARSET), true
    };

    for (int i = 0; i < dataTypes.length; i++) {
      final ConstantOperand operand = new ConstantOperand(dataTypes[i], valueStrings[i]);

      final Column inputColumn = new ConstantInputReader(operand).current()[0];
      assertEquals(
          TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
          inputColumn.getPositionCount());
      assertEquals(expectedValues[i], inputColumn.getObject(0));

      final Column transformedColumn = TransformUtils.transformConstantOperandToColumn(operand);
      assertEquals(1, transformedColumn.getPositionCount());
      assertEquals(expectedValues[i], transformedColumn.getObject(0));
    }
  }

  @Test
  public void testUnsupportedConstantColumnType() {
    final ConstantOperand operand = new ConstantOperand(TSDataType.DATE, "2026-07-17");

    assertThrows(QueryProcessException.class, () -> new ConstantInputReader(operand));
    assertThrows(
        UnSupportedDataTypeException.class,
        () -> TransformUtils.transformConstantOperandToColumn(operand));
  }
}
