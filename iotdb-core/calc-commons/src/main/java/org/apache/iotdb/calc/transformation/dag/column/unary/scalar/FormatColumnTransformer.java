/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.MultiColumnTransformer;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.time.ZoneId;
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.MissingFormatArgumentException;

import static java.lang.String.format;
import static org.apache.iotdb.pipe.api.type.Binary.stringToBytes;

public class FormatColumnTransformer extends MultiColumnTransformer {

  private final ZoneId zoneId;
  private final TypeServices.FormatValueConverter[] valueConverters;

  public FormatColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList, ZoneId zoneId) {
    super(returnType, columnTransformerList);
    this.zoneId = zoneId;
    this.valueConverters = new TypeServices.FormatValueConverter[columnTransformerList.size() - 1];
    for (int i = 0; i < valueConverters.length; i++) {
      valueConverters[i] =
          TypeServices.FORMAT_VALUE_CONVERTER_SERVICE.call(
              columnTransformerList.get(i + 1).getType());
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transform(childrenColumns, builder, i);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transform(childrenColumns, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transform(List<Column> childrenColumns, ColumnBuilder builder, int i) {
    List<Column> valueColumns = childrenColumns.subList(1, childrenColumns.size());
    Object[] values = new Object[valueColumns.size()];
    String pattern = String.valueOf(childrenColumns.get(0).getBinary(i));
    for (int j = 0; j < valueColumns.size(); j++) {
      Column column = valueColumns.get(j);
      if (column.isNull(i)) {
        values[j] = null;
      } else {
        values[j] = valueConverters[j].convert(column, i, zoneId);
      }
    }
    try {
      String formatted = format(pattern, values);
      returnType.writeBinary(builder, new Binary(stringToBytes(formatted)));
    } catch (IllegalFormatConversionException | MissingFormatArgumentException e) {
      String message = e.toString().replaceFirst("^java\\.util\\.(\\w+)Exception", "$1");
      throw new SemanticException(
          String.format("Invalid format string: %s (%s)", pattern, message));
    }
  }

  @Override
  protected void checkType() {
    // do nothing because the type is checked in tableMetaDataImpl
  }
}
