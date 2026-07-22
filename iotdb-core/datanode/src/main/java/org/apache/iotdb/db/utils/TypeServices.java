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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.windowing.window.EvictableBatchList;
import org.apache.iotdb.db.utils.windowing.window.WindowImpl;

import com.google.common.io.BaseEncoding;
import com.sun.jna.platform.win32.Variant;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public class TypeServices {

  public static final int DEFAULT_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));

  public static final TypeService<Function<Object, Column>> CONSTANT_COLUMN_BUILDER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT ->
                value -> {
                  final ColumnBuilder builder = type.createColumnBuilder(1);
                  type.writeObject(builder, value);
                  return builder.build();
                };
            case DATE, TIMESTAMP, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_TYPE + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<IntFunction<ColumnBuilder>>
      TRANSFORMATION_COLUMN_BUILDER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN,
                    INT32,
                    DATE,
                    INT64,
                    TIMESTAMP,
                    FLOAT,
                    DOUBLE,
                    TEXT,
                    BLOB,
                    STRING,
                    OBJECT ->
                    type::createColumnBuilder;
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(
                            String.format(
                                DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT,
                                type.getTypeEnum()))
                        .setChecked(true);
              };

  public static final TypeService<ColumnToDoubleConverter> TRANSFORMATION_VALUE_TO_DOUBLE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> type::getInt;
            case INT64, TIMESTAMP -> type::getLong;
            case FLOAT -> type::getFloat;
            case DOUBLE -> type::getDouble;
            case BOOLEAN -> (column, index) -> type.getBoolean(column, index) ? 1 : 0;
            case TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                (column, index) -> {
                  throw new QueryProcessException(
                      DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum());
                };
          };

  public static final TypeService<StateWindowSplitter> STATE_WINDOW_SPLITTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordInt(type.getInt(values, index - 1));
                    valueRecorder.setRecorded(true);
                  }
                  final boolean split =
                      Math.abs(type.getInt(values, index) - valueRecorder.getInt()) > delta;
                  if (split) {
                    valueRecorder.recordInt(type.getInt(values, index));
                  }
                  return split;
                };
            case INT64 ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordLong(type.getLong(values, index - 1));
                    valueRecorder.setRecorded(true);
                  }
                  final boolean split =
                      Math.abs(type.getLong(values, index) - valueRecorder.getLong()) > delta;
                  if (split) {
                    valueRecorder.recordLong(type.getLong(values, index));
                  }
                  return split;
                };
            case FLOAT ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordFloat(type.getFloat(values, index - 1));
                    valueRecorder.setRecorded(true);
                  }
                  final boolean split =
                      Math.abs(type.getFloat(values, index) - valueRecorder.getFloat()) > delta;
                  if (split) {
                    valueRecorder.recordFloat(type.getFloat(values, index));
                  }
                  return split;
                };
            case DOUBLE ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordDouble(type.getDouble(values, index - 1));
                    valueRecorder.setRecorded(true);
                  }
                  final boolean split =
                      Math.abs(type.getDouble(values, index) - valueRecorder.getDouble()) > delta;
                  if (split) {
                    valueRecorder.recordDouble(type.getDouble(values, index));
                  }
                  return split;
                };
            case BOOLEAN ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordBoolean(type.getBoolean(values, index - 1));
                    valueRecorder.setRecorded(true);
                  }
                  final boolean split =
                      type.getBoolean(values, index) != valueRecorder.getBoolean();
                  if (split) {
                    valueRecorder.recordBoolean(type.getBoolean(values, index));
                  }
                  return split;
                };
            case TEXT ->
                (valueRecorder, delta, values, index) -> {
                  if (!valueRecorder.hasRecorded()) {
                    valueRecorder.recordString(type.getBinary(values, index - 1).toString());
                    valueRecorder.setRecorded(true);
                  }
                  final String value = type.getBinary(values, index).toString();
                  final boolean split = !value.equals(valueRecorder.getString());
                  if (split) {
                    valueRecorder.recordString(value);
                  }
                  return split;
                };
            case DATE, TIMESTAMP, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                (valueRecorder, delta, values, index) -> {
                  throw new UnsupportedOperationException(
                      DataNodeQueryMessages.INVALID_DATA_TYPE_FOR_STATE_WINDOW_STRATEGY);
                };
          };

  public static final TypeService<Function<String, Object>> VALUE_PARSER_NO_EXCEPTION_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> Boolean::parseBoolean;
            case INT32 -> TypeServices::parseInteger;
            case INT64 -> TypeServices::parseLong;
            case FLOAT -> TypeServices::parseFloat;
            case DOUBLE -> TypeServices::parseDouble;
            case TEXT -> TypeServices::parseText;
            case TIMESTAMP -> TypeServices::parseTimestamp;
            case DATE -> TypeServices::parseDate;
            case BLOB -> TypeServices::parseBlob;
            case STRING -> TypeServices::parseString;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type)
                    .setChecked(true);
          };

  public static final TypeService<BiConsumer<Object, IWALByteBufferView>> WAL_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (value, buffer) -> WALWriteUtils.write((Boolean) value, buffer);
            case INT32, DATE -> (value, buffer) -> WALWriteUtils.write((Integer) value, buffer);
            case INT64, TIMESTAMP -> (value, buffer) -> WALWriteUtils.write((Long) value, buffer);
            case FLOAT -> (value, buffer) -> WALWriteUtils.write((Float) value, buffer);
            case DOUBLE -> (value, buffer) -> WALWriteUtils.write((Double) value, buffer);
            case TEXT, BLOB, STRING, OBJECT ->
                (value, buffer) -> WALWriteUtils.write((Binary) value, buffer);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<WALColumnWriter> WAL_ARRAY_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (column, buffer, start, end) -> {
                  int[] values = (int[]) column;
                  for (int i = start; i < end; i++) {
                    buffer.putInt(values[i]);
                  }
                };
            case INT64, TIMESTAMP ->
                (column, buffer, start, end) -> {
                  long[] values = (long[]) column;
                  for (int i = start; i < end; i++) {
                    buffer.putLong(values[i]);
                  }
                };
            case FLOAT ->
                (column, buffer, start, end) -> {
                  float[] values = (float[]) column;
                  for (int i = start; i < end; i++) {
                    buffer.putFloat(values[i]);
                  }
                };
            case DOUBLE ->
                (column, buffer, start, end) -> {
                  double[] values = (double[]) column;
                  for (int i = start; i < end; i++) {
                    buffer.putDouble(values[i]);
                  }
                };
            case BOOLEAN ->
                (column, buffer, start, end) -> {
                  boolean[] values = (boolean[]) column;
                  for (int i = start; i < end; i++) {
                    buffer.put((byte) (values[i] ? 1 : 0));
                  }
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (column, buffer, start, end) -> {
                  Binary[] values = (Binary[]) column;
                  for (int i = start; i < end; i++) {
                    if (values[i] != null && values[i].getValues() != null) {
                      WALWriteUtils.write(values[i], buffer);
                    } else {
                      buffer.putInt(0);
                    }
                  }
                };
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<DecodedValueChunkWriter> DECODED_VALUE_CHUNK_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (writer, time, stream, isNull) ->
                    writer.write(time, isNull ? 0 : ReadWriteIOUtils.readInt(stream), isNull);
            case INT64, TIMESTAMP ->
                (writer, time, stream, isNull) ->
                    writer.write(time, isNull ? 0L : ReadWriteIOUtils.readLong(stream), isNull);
            case FLOAT ->
                (writer, time, stream, isNull) ->
                    writer.write(time, isNull ? 0F : ReadWriteIOUtils.readFloat(stream), isNull);
            case DOUBLE ->
                (writer, time, stream, isNull) ->
                    writer.write(time, isNull ? 0D : ReadWriteIOUtils.readDouble(stream), isNull);
            case BOOLEAN ->
                (writer, time, stream, isNull) ->
                    writer.write(time, !isNull && ReadWriteIOUtils.readBool(stream), isNull);
            case TEXT, BLOB, STRING, OBJECT ->
                (writer, time, stream, isNull) ->
                    writer.write(time, isNull ? null : ReadWriteIOUtils.readBinary(stream), isNull);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<SegmentedArraySerializedSizeCalculator>
      SEGMENTED_ARRAY_SERIALIZED_SIZE_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN -> (valueArrays, rowCount, arraySize) -> rowCount * Byte.BYTES;
                case INT32, DATE -> (valueArrays, rowCount, arraySize) -> rowCount * Integer.BYTES;
                case INT64, TIMESTAMP ->
                    (valueArrays, rowCount, arraySize) -> rowCount * Long.BYTES;
                case FLOAT -> (valueArrays, rowCount, arraySize) -> rowCount * Float.BYTES;
                case DOUBLE -> (valueArrays, rowCount, arraySize) -> rowCount * Double.BYTES;
                case TEXT, BLOB, STRING, OBJECT ->
                    (valueArrays, rowCount, arraySize) -> {
                      int size = 0;
                      int remaining = rowCount;
                      for (Object valueArray : valueArrays) {
                        int length = Math.min(remaining, arraySize);
                        size += type.serializedSize(valueArray, length);
                        remaining -= length;
                        if (remaining == 0) {
                          break;
                        }
                      }
                      return size;
                    };
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                        .setChecked(true);
              };

  public static final TypeService<ArrayValueColumnWriter> ARRAY_VALUE_COLUMN_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (builder, values, index, floatPrecision, encoding) ->
                    builder.writeBoolean(((boolean[]) values)[index]);
            case INT32 ->
                (builder, values, index, floatPrecision, encoding) ->
                    builder.writeInt(((int[]) values)[index]);
            case DATE ->
                (builder, values, index, floatPrecision, encoding) -> {
                  int value = ((int[]) values)[index];
                  if (builder instanceof BinaryColumnBuilder) {
                    ((BinaryColumnBuilder) builder).writeDate(value);
                  } else {
                    builder.writeInt(value);
                  }
                };
            case INT64, TIMESTAMP ->
                (builder, values, index, floatPrecision, encoding) ->
                    builder.writeLong(((long[]) values)[index]);
            case FLOAT ->
                (builder, values, index, floatPrecision, encoding) -> {
                  float value = ((float[]) values)[index];
                  if (encoding != null
                      && !Float.isNaN(value)
                      && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
                    value = MathUtils.roundWithGivenPrecision(value, floatPrecision);
                  }
                  builder.writeFloat(value);
                };
            case DOUBLE ->
                (builder, values, index, floatPrecision, encoding) -> {
                  double value = ((double[]) values)[index];
                  if (encoding != null
                      && !Double.isNaN(value)
                      && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
                    value = MathUtils.roundWithGivenPrecision(value, floatPrecision);
                  }
                  builder.writeDouble(value);
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (builder, values, index, floatPrecision, encoding) ->
                    builder.writeBinary(((Binary[]) values)[index]);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<ValueSerializer<Object>> OBJECT_VALUE_SERIALIZER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> (value, stream) -> ReadWriteIOUtils.write((int) value, stream);
            case INT64, TIMESTAMP ->
                (value, stream) -> ReadWriteIOUtils.write((long) value, stream);
            case FLOAT -> (value, stream) -> ReadWriteIOUtils.write((float) value, stream);
            case DOUBLE -> (value, stream) -> ReadWriteIOUtils.write((double) value, stream);
            case BOOLEAN -> (value, stream) -> ReadWriteIOUtils.write((boolean) value, stream);
            case TEXT, BLOB, STRING, OBJECT ->
                (value, stream) -> ReadWriteIOUtils.write((Binary) value, stream);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<ValueSerializer<TsPrimitiveType>>
      TS_PRIMITIVE_VALUE_SERIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (value, stream) -> ReadWriteIOUtils.write(value.getInt(), stream);
                case INT64, TIMESTAMP ->
                    (value, stream) -> ReadWriteIOUtils.write(value.getLong(), stream);
                case FLOAT -> (value, stream) -> ReadWriteIOUtils.write(value.getFloat(), stream);
                case DOUBLE -> (value, stream) -> ReadWriteIOUtils.write(value.getDouble(), stream);
                case BOOLEAN ->
                    (value, stream) -> ReadWriteIOUtils.write(value.getBoolean(), stream);
                case TEXT, BLOB, STRING, OBJECT ->
                    (value, stream) -> ReadWriteIOUtils.write(value.getBinary(), stream);
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                        .setChecked(true);
              };

  public static final TypeService<Supplier<TsPrimitiveType>>
      EMPTY_TS_PRIMITIVE_TYPE_FACTORY_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> type::getTsPrimitiveType;
                case DATE -> Type.fromTsDataType(TSDataType.INT32)::getTsPrimitiveType;
                case TIMESTAMP -> Type.fromTsDataType(TSDataType.INT64)::getTsPrimitiveType;
                case TEXT, BLOB, STRING, OBJECT ->
                    () ->
                        Type.fromTsDataType(TSDataType.TEXT)
                            .getTsPrimitiveType(new Binary("", TSFileConfig.STRING_CHARSET));
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                        .setChecked(true);
              };

  public static final TypeService<Function<IoTDBConfig, TSEncoding>> DEFAULT_ENCODING_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> IoTDBConfig::getDefaultBooleanEncoding;
            case INT32, DATE -> IoTDBConfig::getDefaultInt32Encoding;
            case INT64, TIMESTAMP -> IoTDBConfig::getDefaultInt64Encoding;
            case FLOAT -> IoTDBConfig::getDefaultFloatEncoding;
            case DOUBLE -> IoTDBConfig::getDefaultDoubleEncoding;
            case TEXT, BLOB, STRING, OBJECT -> IoTDBConfig::getDefaultTextEncoding;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<TabletColumnDecoder> TABLET_COLUMN_DECODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (decoder, buffer, rowCount, encoding) -> {
                  int[] values = new int[rowCount];
                  // PlainEncoder uses var int, which may cause compatibility problems.
                  for (int i = 0; i < rowCount; i++) {
                    values[i] =
                        encoding == TSEncoding.PLAIN
                            ? ReadWriteIOUtils.readInt(buffer)
                            : decoder.readInt(buffer);
                  }
                  return values;
                };
            case INT64, TIMESTAMP ->
                (decoder, buffer, rowCount, encoding) -> {
                  long[] values = new long[rowCount];
                  for (int i = 0; i < rowCount; i++) {
                    values[i] = decoder.readLong(buffer);
                  }
                  return values;
                };
            case FLOAT ->
                (decoder, buffer, rowCount, encoding) -> {
                  float[] values = new float[rowCount];
                  for (int i = 0; i < rowCount; i++) {
                    values[i] = decoder.readFloat(buffer);
                  }
                  return values;
                };
            case DOUBLE ->
                (decoder, buffer, rowCount, encoding) -> {
                  double[] values = new double[rowCount];
                  for (int i = 0; i < rowCount; i++) {
                    values[i] = decoder.readDouble(buffer);
                  }
                  return values;
                };
            case BOOLEAN ->
                (decoder, buffer, rowCount, encoding) -> {
                  boolean[] values = new boolean[rowCount];
                  for (int i = 0; i < rowCount; i++) {
                    values[i] = decoder.readBoolean(buffer);
                  }
                  return values;
                };
            case TEXT, BLOB, STRING ->
                (decoder, buffer, rowCount, encoding) -> {
                  Binary[] values = new Binary[rowCount];
                  // PlainEncoder uses var int, which may cause compatibility problems.
                  for (int i = 0; i < rowCount; i++) {
                    values[i] =
                        encoding == TSEncoding.PLAIN
                            ? ReadWriteIOUtils.readBinary(buffer)
                            : decoder.readBinary(buffer);
                  }
                  return values;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<DecodedArrayValueReader> DECODED_ARRAY_VALUE_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (values, index, stream) ->
                    ((int[]) values)[index] = ReadWriteIOUtils.readInt(stream);
            case INT64, TIMESTAMP ->
                (values, index, stream) ->
                    ((long[]) values)[index] = ReadWriteIOUtils.readLong(stream);
            case FLOAT ->
                (values, index, stream) ->
                    ((float[]) values)[index] = ReadWriteIOUtils.readFloat(stream);
            case DOUBLE ->
                (values, index, stream) ->
                    ((double[]) values)[index] = ReadWriteIOUtils.readDouble(stream);
            case BOOLEAN ->
                (values, index, stream) ->
                    ((boolean[]) values)[index] = ReadWriteIOUtils.readBool(stream);
            case TEXT, BLOB, STRING, OBJECT ->
                (values, index, stream) ->
                    ((Binary[]) values)[index] = ReadWriteIOUtils.readBinary(stream);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<DecodedChunkWriter> DECODED_CHUNK_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readInt(stream));
            case INT64, TIMESTAMP ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readLong(stream));
            case FLOAT ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readFloat(stream));
            case DOUBLE ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readDouble(stream));
            case BOOLEAN ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readBool(stream));
            case TEXT, BLOB, STRING, OBJECT ->
                (writer, time, stream) -> writer.write(time, ReadWriteIOUtils.readBinary(stream));
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<TVListArrayWriter> TV_LIST_ARRAY_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putBooleans(times, (boolean[]) values, bitMap, start, end);
            case INT32, DATE ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putInts(times, (int[]) values, bitMap, start, end);
            case INT64, TIMESTAMP ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putLongs(times, (long[]) values, bitMap, start, end);
            case FLOAT ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putFloats(times, (float[]) values, bitMap, start, end);
            case DOUBLE ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putDoubles(times, (double[]) values, bitMap, start, end);
            case TEXT, BLOB, STRING, OBJECT ->
                (tvList, times, values, bitMap, start, end) ->
                    tvList.putBinaries(times, (Binary[]) values, bitMap, start, end);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<IntFunction<Object>> PRIMITIVE_ARRAY_ALLOCATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, TEXT, BLOB, STRING, OBJECT ->
                type::createArray;
            case DATE -> Type.fromTsDataType(TSDataType.INT32)::createArray;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(type.getTypeEnum().name()).setChecked(true);
          };

  public static final TypeService<IntFunction<Object>> TABLET_COLUMN_ALLOCATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN,
                INT32,
                DATE,
                INT64,
                TIMESTAMP,
                FLOAT,
                DOUBLE,
                TEXT,
                BLOB,
                STRING,
                OBJECT ->
                type::createArray;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(type.getTypeEnum().name()).setChecked(true);
          };

  public static final TypeService<IntFunction<Object>> EMPTY_TABLET_COLUMN_FACTORY_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE -> type::createArray;
            case DATE ->
                size -> {
                  LocalDate[] values = (LocalDate[]) type.createArray(size);
                  Arrays.fill(values, LocalDate.of(1000, 1, 1));
                  return values;
                };
            case TEXT, BLOB, STRING ->
                size -> {
                  Binary[] values = (Binary[]) type.createArray(size);
                  Arrays.fill(values, Binary.EMPTY_VALUE);
                  return values;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(type.getTypeEnum().name()).setChecked(true);
          };

  public static final TypeService<WindowValueArrayBuilder> WINDOW_VALUE_ARRAY_BUILDER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (window, list, begin, size) -> {
                  int[] values = new int[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getIntByIndex(begin + i);
                  }
                  window.setIntValues(values);
                };
            case INT64, TIMESTAMP ->
                (window, list, begin, size) -> {
                  long[] values = new long[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getLongByIndex(begin + i);
                  }
                  window.setLongValues(values);
                };
            case FLOAT ->
                (window, list, begin, size) -> {
                  float[] values = new float[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getFloatByIndex(begin + i);
                  }
                  window.setFloatValues(values);
                };
            case DOUBLE ->
                (window, list, begin, size) -> {
                  double[] values = new double[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getDoubleByIndex(begin + i);
                  }
                  window.setDoubleValues(values);
                };
            case BOOLEAN ->
                (window, list, begin, size) -> {
                  boolean[] values = new boolean[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getBooleanByIndex(begin + i);
                  }
                  window.setBooleanValues(values);
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (window, list, begin, size) -> {
                  Binary[] values = new Binary[size];
                  for (int i = 0; i < size; i++) {
                    values[i] = list.getBinaryByIndex(begin + i);
                  }
                  window.setBinaryValues(values);
                };
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(type.getTypeEnum().name()).setChecked(true);
          };

  public static final TypeService<BiFunction<ByteBuffer, Integer, Object>>
      RAW_ARRAY_BYTE_BUFFER_DESERIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, TEXT, BLOB, STRING, OBJECT ->
                    type::deserializeArray;
                case DATE -> Type.fromTsDataType(TSDataType.INT32)::deserializeArray;
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                        .setChecked(true);
              };

  public static final TypeService<RawArrayInputStreamDeserializer>
      RAW_ARRAY_INPUT_STREAM_DESERIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, TEXT, BLOB, STRING, OBJECT ->
                    type::deserializeArray;
                case DATE -> Type.fromTsDataType(TSDataType.INT32)::deserializeArray;
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                        .setChecked(true);
              };

  public static final TypeService<ArrayValueGetter> ARRAY_VALUE_GETTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (array, index) -> ((boolean[]) array)[index];
            case INT32, DATE -> (array, index) -> ((int[]) array)[index];
            case INT64, TIMESTAMP -> (array, index) -> ((long[]) array)[index];
            case FLOAT -> (array, index) -> ((float[]) array)[index];
            case DOUBLE -> (array, index) -> ((double[]) array)[index];
            case TEXT, BLOB, STRING, OBJECT -> (array, index) -> ((Binary[]) array)[index];
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(type.getTypeEnum().name()).setChecked(true);
          };

  public static final TypeService<Function<String, Comparable<?>>>
      CONVERT_PREDICATE_VALUE_PARSER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 -> Integer::valueOf;
                case INT64, TIMESTAMP -> Long::valueOf;
                case FLOAT -> Float::valueOf;
                case DOUBLE -> Double::valueOf;
                case BOOLEAN ->
                    valueString -> {
                      if (valueString.equalsIgnoreCase("true")) {
                        return Boolean.TRUE;
                      } else if (valueString.equalsIgnoreCase("false")) {
                        return Boolean.FALSE;
                      }
                      throw new IllegalArgumentException(
                          String.format(
                              DataNodeQueryMessages.VALUE_CANNOT_BE_CAST_TO_DATA_TYPE_FMT,
                              valueString,
                              type.getTypeEnum()));
                    };
                case BLOB -> valueString -> new Binary(BaseEncoding.base16().decode(valueString));
                case TEXT, STRING ->
                    valueString -> new Binary(valueString, TSFileConfig.STRING_CHARSET);
                case DATE -> DateTimeUtils::parseDateExpressionToInt;
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    throw new UnsupportedOperationException(
                        String.format(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, type.getTypeEnum()));
              };

  public static final TypeService<Function<Literal, Comparable<?>>>
      RELATIONAL_CONVERT_PREDICATE_VALUE_PARSER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 -> value -> Integer.valueOf((int) getLongValue(value));
                case DATE -> value -> Integer.valueOf(((GenericLiteral) value).getValue());
                case INT64 -> value -> Long.valueOf(getLongValue(value));
                case TIMESTAMP -> TypeServices::getTimestampValue;
                case FLOAT -> value -> Float.valueOf((float) getDoubleValue(value));
                case DOUBLE -> value -> Double.valueOf(getDoubleValue(value));
                case BOOLEAN -> value -> Boolean.valueOf(((BooleanLiteral) value).getValue());
                case TEXT, STRING ->
                    value ->
                        new Binary(((StringLiteral) value).getValue(), TSFileConfig.STRING_CHARSET);
                case BLOB -> value -> new Binary(((BinaryLiteral) value).getValue());
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    throw new UnsupportedOperationException(
                        String.format(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, type.getTypeEnum()));
              };

  public static final TypeService<Function<Column, Literal>>
      UNCORRELATED_SCALAR_SUBQUERY_RESULT_LITERAL_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    column -> new LongLiteral(Long.toString(type.getInt(column, 0)));
                case INT64, TIMESTAMP ->
                    column -> new LongLiteral(Long.toString(type.getLong(column, 0)));
                case FLOAT -> column -> new FloatLiteral(type.getFloat(column, 0));
                case DOUBLE ->
                    column -> new DoubleLiteral(Double.toString(type.getDouble(column, 0)));
                case BOOLEAN ->
                    column -> new BooleanLiteral(Boolean.toString(type.getBoolean(column, 0)));
                case BLOB -> column -> new BinaryLiteral(type.getBinary(column, 0).toString());
                case TEXT, STRING ->
                    column -> new StringLiteral(type.getBinary(column, 0).toString());
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    throw new IllegalArgumentException(
                        String.format(
                            DataNodeQueryMessages.UNSUPPORTED_SCALAR_SUBQUERY_RESULT_DATA_TYPE_FMT,
                            type.getTypeEnum()));
              };

  public static final TypeService<Short> OPC_DA_VARIANT_TYPE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> Variant.VT_BOOL;
            case INT32 -> Variant.VT_I4;
            case INT64 -> Variant.VT_I8;
            case DATE, TIMESTAMP -> Variant.VT_DATE;
            case FLOAT -> Variant.VT_R4;
            case DOUBLE -> Variant.VT_R8;
            // Note that "Variant" does not support "VT_BLOB" data, and not all the DA servers
            // support this, thus we use "VT_BSTR" to substitute.
            case TEXT, STRING, BLOB, OBJECT -> Variant.VT_BSTR;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum())
                    .setChecked(true);
          };

  public static final TypeService<Function<Object, String>> OPC_UA_VALUE_STRINGIFIER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, BLOB, STRING -> Object::toString;
            case DATE ->
                value -> ((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toString();
            case TIMESTAMP -> value -> DateTimeUtils.convertLongToDate((long) value);
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                value -> {
                  throw new PipeRuntimeNonCriticalException(
                      DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<Function<Boolean, Type>>
      PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, TEXT, BLOB, OBJECT, STRING ->
                    ignored -> type;
                case DATE ->
                    isDateStoredAsLocalDate ->
                        isDateStoredAsLocalDate ? type : Type.fromTsDataType(TSDataType.INT32);
                case ROW, UNKNOWN, VECTOR ->
                    ignored -> {
                      throw new UnSupportedDataTypeException(
                              DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                          .setChecked(true);
                    };
              };

  static {
    OPC_UA_VALUE_STRINGIFIER_SERVICE.check();
    PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE.check();
    TV_LIST_ARRAY_WRITER_SERVICE.check();
    PRIMITIVE_ARRAY_ALLOCATOR_SERVICE.check();
    TABLET_COLUMN_ALLOCATOR_SERVICE.check();
    EMPTY_TABLET_COLUMN_FACTORY_SERVICE.check();
    WINDOW_VALUE_ARRAY_BUILDER_SERVICE.check();
    RAW_ARRAY_BYTE_BUFFER_DESERIALIZER_SERVICE.check();
    RAW_ARRAY_INPUT_STREAM_DESERIALIZER_SERVICE.check();
    ARRAY_VALUE_GETTER_SERVICE.check();
    DECODED_VALUE_CHUNK_WRITER_SERVICE.check();
    SEGMENTED_ARRAY_SERIALIZED_SIZE_SERVICE.check();
    ARRAY_VALUE_COLUMN_WRITER_SERVICE.check();
    OBJECT_VALUE_SERIALIZER_SERVICE.check();
    TS_PRIMITIVE_VALUE_SERIALIZER_SERVICE.check();
    EMPTY_TS_PRIMITIVE_TYPE_FACTORY_SERVICE.check();
    DEFAULT_ENCODING_SERVICE.check();
    TABLET_COLUMN_DECODER_SERVICE.check();
    DECODED_ARRAY_VALUE_READER_SERVICE.check();
    DECODED_CHUNK_WRITER_SERVICE.check();
  }

  public static int parseInteger(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (final Exception e) {
      return 0;
    }
  }

  public static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static float parseFloat(final String value) {
    try {
      return Float.parseFloat(value);
    } catch (final Exception e) {
      return 0.0f;
    }
  }

  public static double parseDouble(final String value) {
    try {
      return Double.parseDouble(value);
    } catch (final Exception e) {
      return 0.0d;
    }
  }

  public static Binary parseBlob(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseString(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseText(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static long parseTimestamp(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Long.parseLong(value)
          : DataNodeDateTimeUtils.parseDateTimeExpressionToLong(
              StringUtils.trim(value), ZoneOffset.UTC);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static int parseDate(final String value) {
    if (value == null) {
      return DEFAULT_DATE;
    }
    final String trimmedValue = StringUtils.trim(value);
    if (trimmedValue.isEmpty()) {
      return DEFAULT_DATE;
    }
    if (TypeInferenceUtils.isNumber(trimmedValue)) {
      try {
        int date = Integer.parseInt(trimmedValue);
        DateUtils.parseIntToLocalDate(date);
        return date;
      } catch (final Exception e) {
        return DEFAULT_DATE;
      }
    }
    try {
      return DateTimeUtils.parseDateExpressionToInt(trimmedValue);
    } catch (final Exception e) {
      return parseDateTimeToDate(trimmedValue);
    }
  }

  public static int parseDateTimeToDate(final String value) {
    try {
      return DateUtils.parseDateExpressionToInt(
          Instant.ofEpochMilli(
                  DateTimeUtils.convertDatetimeStrToLong(value, ZoneOffset.UTC, 0, "ms"))
              .atZone(ZoneOffset.UTC)
              .toLocalDate());
    } catch (final Exception e) {
      return DEFAULT_DATE;
    }
  }

  private static long getLongValue(final Literal value) {
    return ((LongLiteral) value).getParsedValue();
  }

  private static double getDoubleValue(final Literal value) {
    if (value instanceof DoubleLiteral) {
      return ((DoubleLiteral) value).getValue();
    } else if (value instanceof LongLiteral) {
      return ((LongLiteral) value).getParsedValue();
    } else if (value instanceof FloatLiteral) {
      return ((FloatLiteral) value).getValue();
    }
    throw new IllegalArgumentException(
        DataNodeQueryMessages.EXPRESSION_SHOULD_BE_NUMERIC_ACTUAL_IS + value);
  }

  private static Long getTimestampValue(final Literal value) {
    if (value instanceof LongLiteral) {
      return ((LongLiteral) value).getParsedValue();
    } else if (value instanceof DoubleLiteral) {
      return (long) ((DoubleLiteral) value).getValue();
    } else if (value instanceof GenericLiteral) {
      return Long.valueOf(((GenericLiteral) value).getValue());
    }
    throw new SemanticException(
        String.format(
            DataNodeQueryMessages.TIMESTAMP_IN_LIST_LITERAL_TYPE_ERROR_FMT,
            value.getClass().getSimpleName()));
  }

  @FunctionalInterface
  public interface ColumnToDoubleConverter {
    double convert(Column column, int index) throws QueryProcessException;
  }

  @FunctionalInterface
  public interface StateWindowSplitter {
    boolean split(ValueRecorder valueRecorder, double delta, Column values, int index);
  }

  @FunctionalInterface
  public interface WALColumnWriter {
    void write(Object column, IWALByteBufferView buffer, int start, int end);
  }

  @FunctionalInterface
  public interface TVListArrayWriter {
    void write(TVList tvList, long[] times, Object values, BitMap bitMap, int start, int end);
  }

  @FunctionalInterface
  public interface ArrayValueGetter {
    Object get(Object array, int index);
  }

  @FunctionalInterface
  public interface DecodedValueChunkWriter {
    void write(ValueChunkWriter writer, long time, InputStream stream, boolean isNull)
        throws IOException;
  }

  @FunctionalInterface
  public interface SegmentedArraySerializedSizeCalculator {
    int calculate(List<Object> valueArrays, int rowCount, int arraySize);
  }

  @FunctionalInterface
  public interface ArrayValueColumnWriter {
    void write(
        ColumnBuilder builder, Object values, int index, int floatPrecision, TSEncoding encoding);
  }

  @FunctionalInterface
  public interface ValueSerializer<T> {
    int serialize(T value, DataOutputStream stream) throws IOException;
  }

  @FunctionalInterface
  public interface TabletColumnDecoder {
    Object decode(Decoder decoder, ByteBuffer buffer, int rowCount, TSEncoding encoding);
  }

  @FunctionalInterface
  public interface DecodedArrayValueReader {
    void read(Object values, int index, InputStream stream) throws IOException;
  }

  @FunctionalInterface
  public interface RawArrayInputStreamDeserializer {
    Object deserialize(DataInputStream stream, int size) throws IOException;
  }

  @FunctionalInterface
  public interface WindowValueArrayBuilder {
    void build(WindowImpl window, EvictableBatchList list, int begin, int size);
  }

  @FunctionalInterface
  public interface DecodedChunkWriter {
    void write(ChunkWriterImpl writer, long time, InputStream stream) throws IOException;
  }
}
