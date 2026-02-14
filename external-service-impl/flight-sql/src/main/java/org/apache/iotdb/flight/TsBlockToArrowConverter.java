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

package org.apache.iotdb.flight;

import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Converts IoTDB TsBlock data to Apache Arrow VectorSchemaRoot format. */
public class TsBlockToArrowConverter {

  private TsBlockToArrowConverter() {
    // utility class
  }

  /** Maps IoTDB TSDataType to Apache Arrow ArrowType. */
  public static ArrowType toArrowType(TSDataType tsDataType) {
    switch (tsDataType) {
      case BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case INT32:
        return new ArrowType.Int(32, true);
      case INT64:
        return new ArrowType.Int(64, true);
      case FLOAT:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case TEXT:
      case STRING:
        return ArrowType.Utf8.INSTANCE;
      case BLOB:
        return ArrowType.Binary.INSTANCE;
      case TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
      case DATE:
        return new ArrowType.Date(DateUnit.DAY);
      default:
        throw new IllegalArgumentException("Unsupported TSDataType: " + tsDataType);
    }
  }

  /** Builds an Arrow Schema from an IoTDB DatasetHeader. */
  public static Schema toArrowSchema(DatasetHeader header) {
    List<String> columnNames = header.getRespColumns();
    List<TSDataType> dataTypes = header.getRespDataTypes();
    List<Field> fields = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      ArrowType arrowType = toArrowType(dataTypes.get(i));
      fields.add(new Field(columnNames.get(i), new FieldType(true, arrowType, null), null));
    }
    return new Schema(fields);
  }

  /** Creates a new VectorSchemaRoot from an IoTDB DatasetHeader. */
  public static VectorSchemaRoot createVectorSchemaRoot(
      DatasetHeader header, BufferAllocator allocator) {
    Schema arrowSchema = toArrowSchema(header);
    return VectorSchemaRoot.create(arrowSchema, allocator);
  }

  /**
   * Fills an existing VectorSchemaRoot with data from a TsBlock.
   *
   * @param root the VectorSchemaRoot to fill (cleared before filling)
   * @param tsBlock the TsBlock containing the data
   * @param header the DatasetHeader for column name to index mapping
   */
  public static void fillVectorSchemaRoot(
      VectorSchemaRoot root, TsBlock tsBlock, DatasetHeader header) {

    int rowCount = tsBlock.getPositionCount();
    List<String> columnNames = header.getRespColumns();
    List<TSDataType> dataTypes = header.getRespDataTypes();
    Map<String, Integer> headerMap = header.getColumnNameIndexMap();

    root.allocateNew();

    for (int colIdx = 0; colIdx < columnNames.size(); colIdx++) {
      String colName = columnNames.get(colIdx);
      int sourceIdx =
          (headerMap != null && headerMap.containsKey(colName))
              ? headerMap.get(colName)
              : colIdx;
      Column column = tsBlock.getColumn(sourceIdx);
      TSDataType dataType = dataTypes.get(colIdx);
      FieldVector fieldVector = root.getVector(colIdx);

      fillColumnVector(fieldVector, column, dataType, rowCount);
    }

    root.setRowCount(rowCount);
  }

  /** Fills an Arrow FieldVector from an IoTDB Column. */
  private static void fillColumnVector(
      FieldVector fieldVector, Column column, TSDataType dataType, int rowCount) {
    switch (dataType) {
      case BOOLEAN:
        fillBooleanVector((BitVector) fieldVector, column, rowCount);
        break;
      case INT32:
        fillInt32Vector((IntVector) fieldVector, column, rowCount);
        break;
      case INT64:
        fillInt64Vector((BigIntVector) fieldVector, column, rowCount);
        break;
      case FLOAT:
        fillFloatVector((Float4Vector) fieldVector, column, rowCount);
        break;
      case DOUBLE:
        fillDoubleVector((Float8Vector) fieldVector, column, rowCount);
        break;
      case TEXT:
      case STRING:
        fillTextVector((VarCharVector) fieldVector, column, rowCount);
        break;
      case BLOB:
        fillBlobVector((VarBinaryVector) fieldVector, column, rowCount);
        break;
      case TIMESTAMP:
        fillTimestampVector((TimeStampMilliTZVector) fieldVector, column, rowCount);
        break;
      case DATE:
        fillDateVector((DateDayVector) fieldVector, column, rowCount);
        break;
      default:
        throw new IllegalArgumentException("Unsupported TSDataType: " + dataType);
    }
  }

  private static void fillBooleanVector(BitVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getBoolean(i) ? 1 : 0);
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillInt32Vector(IntVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getInt(i));
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillInt64Vector(BigIntVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getLong(i));
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillFloatVector(Float4Vector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getFloat(i));
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillDoubleVector(Float8Vector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getDouble(i));
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillTextVector(VarCharVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        byte[] bytes =
            column
                .getBinary(i)
                .getStringValue(TSFileConfig.STRING_CHARSET)
                .getBytes(StandardCharsets.UTF_8);
        vector.setSafe(i, bytes);
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillBlobVector(VarBinaryVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getBinary(i).getValues());
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillTimestampVector(
      TimeStampMilliTZVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getLong(i));
      }
    }
    vector.setValueCount(rowCount);
  }

  private static void fillDateVector(DateDayVector vector, Column column, int rowCount) {
    for (int i = 0; i < rowCount; i++) {
      if (column.isNull(i)) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, column.getInt(i));
      }
    }
    vector.setValueCount(rowCount);
  }
}
