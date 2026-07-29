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
import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.calc.execution.aggregation.BinaryModeAccumulator;
import org.apache.iotdb.calc.execution.aggregation.BooleanModeAccumulator;
import org.apache.iotdb.calc.execution.aggregation.DoubleModeAccumulator;
import org.apache.iotdb.calc.execution.aggregation.FloatModeAccumulator;
import org.apache.iotdb.calc.execution.aggregation.IntModeAccumulator;
import org.apache.iotdb.calc.execution.aggregation.LongModeAccumulator;
import org.apache.iotdb.calc.execution.operator.process.fill.IFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.transformation.dag.util.CastFunctionUtils;
import org.apache.iotdb.calc.utils.constant.SqlConstant;
import org.apache.iotdb.commons.conf.CommonDescriptor;
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
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.AbstractSameTypeNumericOperator;
import org.apache.iotdb.db.queryengine.statistics.StatisticsManager;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.ArithmeticNegationTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.InTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar.DiffFunctionTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar.RoundFunctionTransformer;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.BinaryTVList;
import org.apache.iotdb.db.utils.datastructure.BooleanTVList;
import org.apache.iotdb.db.utils.datastructure.DoubleTVList;
import org.apache.iotdb.db.utils.datastructure.FloatTVList;
import org.apache.iotdb.db.utils.datastructure.IntTVList;
import org.apache.iotdb.db.utils.datastructure.LongTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.windowing.window.EvictableBatchList;
import org.apache.iotdb.db.utils.windowing.window.WindowImpl;
import org.apache.iotdb.rpc.RpcUtils;

import com.google.common.io.BaseEncoding;
import com.sun.jna.platform.win32.OaIdl;
import com.sun.jna.platform.win32.OleAuto;
import com.sun.jna.platform.win32.Variant;
import com.sun.jna.platform.win32.WTypes;
import com.sun.jna.platform.win32.WinDef;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class TypeServices {

  public static final int DEFAULT_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));
  private static final LocalDate EMPTY_LOCAL_DATE = LocalDate.of(1000, 1, 1);

  public static final class Memory {

    public static final TypeService<LongSupplier> STATEMENT_VALUE_SIZE_PER_LINE_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN, INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE -> type::estimateValueSize;
              case TEXT, BLOB, STRING ->
                  () -> StatisticsManager.getInstance().getMaxBinarySizeInBytes();
              case OBJECT, ROW, UNKNOWN, VECTOR ->
                  () -> {
                    throw new UnsupportedOperationException(
                        CalcMessages.UNKNOWN_DATATYPE + type.getTypeEnum());
                  };
            };

    public static final TypeService<ToLongFunction<Object>> INSERT_NODE_VALUE_SIZE_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN, INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE ->
                  value ->
                      RamUsageEstimator.alignObjectSize(
                          type.estimateValueSize() + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER);
              case TEXT, BLOB, STRING ->
                  value -> Objects.nonNull(value) ? ((Binary) value).ramBytesUsed() : 0L;
              case OBJECT, ROW, UNKNOWN, VECTOR -> value -> 0L;
            };

    static {
      STATEMENT_VALUE_SIZE_PER_LINE_SERVICE.check();
      INSERT_NODE_VALUE_SIZE_SERVICE.check();
    }

    private Memory() {
      // Utility class
    }
  }

  public static final class Transformation {

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

    public static final TypeService<
            Function<org.apache.iotdb.db.queryengine.plan.statement.literal.Literal, IFill>>
        CONSTANT_FILL_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN -> literal -> new BooleanConstantFill(literal.getBoolean());
                  case INT32 -> literal -> new IntConstantFill(literal.getInt());
                  case DATE -> literal -> new IntConstantFill(literal.getDate());
                  case INT64, TIMESTAMP -> literal -> new LongConstantFill(literal.getLong());
                  case FLOAT -> literal -> new FloatConstantFill(literal.getFloat());
                  case DOUBLE -> literal -> new DoubleConstantFill(literal.getDouble());
                  case TEXT, BLOB, STRING -> literal -> new BinaryConstantFill(literal.getBinary());
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      literal -> {
                        throw new IllegalArgumentException(
                            CommonOperatorUtils.UNKNOWN_DATATYPE + type.getTypeEnum());
                      };
                };

    public static final TypeService<IntFunction<ColumnBuilder>> COLUMN_BUILDER_SERVICE =
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
                              DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, type.getTypeEnum()))
                      .setChecked(true);
            };

    public static final TypeService<ColumnToDoubleConverter> VALUE_TO_DOUBLE_SERVICE =
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

    public static final TypeService<ColumnValueWriter> TRANSFORM_COLUMN_VALUE_WRITER_SERVICE =
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
                  type::write;
              case ROW, UNKNOWN, VECTOR ->
                  (builder, column, index) -> {
                    throw new UnSupportedDataTypeException(
                        String.format(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_FMT, type.getTypeEnum()));
                  };
            };

    public static final TypeService<RoundTransformer> ROUND_TRANSFORMER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case INT32 -> RoundFunctionTransformer::transformInt;
              case INT64 -> RoundFunctionTransformer::transformLong;
              case FLOAT -> RoundFunctionTransformer::transformFloat;
              case DOUBLE -> RoundFunctionTransformer::transformDouble;
              case BOOLEAN, DATE, TIMESTAMP, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                  (transformer, columns, builder) -> {
                    throw new UnsupportedOperationException(
                        String.format("Unsupported source dataType: %s", type.getTypeEnum()));
                  };
            };

    public static final TypeService<DiffTransformer> DIFF_TRANSFORMER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case INT32 -> DiffFunctionTransformer::transformInt;
              case INT64 -> DiffFunctionTransformer::transformLong;
              case FLOAT -> DiffFunctionTransformer::transformFloat;
              case DOUBLE -> DiffFunctionTransformer::transformDouble;
              case BOOLEAN, DATE, TIMESTAMP, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                  (transformer, columns, builder) -> {
                    throw new QueryProcessException(
                        DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_2 + type.getTypeEnum());
                  };
            };

    public static final TypeService<NegationTransformer> NEGATION_TRANSFORMER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case INT32 -> ArithmeticNegationTransformer::transformInt;
              case INT64 -> ArithmeticNegationTransformer::transformLong;
              case FLOAT -> ArithmeticNegationTransformer::transformFloat;
              case DOUBLE -> ArithmeticNegationTransformer::transformDouble;
              case BOOLEAN, DATE, TIMESTAMP, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                  (transformer, columns, builder) -> {
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

    public static final TypeService<InTransformerSetInitializer>
        IN_TRANSFORMER_SET_INITIALIZER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32, DATE -> InTransformer::initIntSet;
                  case INT64, TIMESTAMP -> InTransformer::initLongSet;
                  case FLOAT -> InTransformer::initFloatSet;
                  case DOUBLE -> InTransformer::initDoubleSet;
                  case BOOLEAN -> InTransformer::initBooleanSet;
                  case TEXT, STRING -> InTransformer::initStringSet;
                  case BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                      (transformer, values) -> {
                        throw new UnsupportedOperationException(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_3 + type.getTypeEnum());
                      };
                };

    public static final TypeService<InTransformerColumnTransformer>
        IN_TRANSFORMER_COLUMN_TRANSFORMER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32, DATE -> InTransformer::transformInt;
                  case INT64, TIMESTAMP -> InTransformer::transformLong;
                  case FLOAT -> InTransformer::transformFloat;
                  case DOUBLE -> InTransformer::transformDouble;
                  case BOOLEAN -> InTransformer::transformBoolean;
                  case TEXT, STRING -> InTransformer::transformBinary;
                  case BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                      (transformer, columns, builder) -> {
                        throw new QueryProcessException(
                            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_3 + type.getTypeEnum());
                      };
                };

    public static final TypeService<TypeService<CastColumnStrategy>> CAST_COLUMN_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case INT32 ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeInt(builder, type.getInt(column, index)));
                        case INT64 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeLong(builder, type.getInt(column, index)));
                        case FLOAT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeFloat(builder, type.getInt(column, index)));
                        case DOUBLE ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeDouble(builder, type.getInt(column, index)));
                        case BOOLEAN ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBoolean(
                                        builder, type.getInt(column, index) != 0));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(
                                        builder,
                                        BytesUtils.valueOf(
                                            String.valueOf(type.getInt(column, index)))));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case INT64 ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeInt(
                                        builder,
                                        CastFunctionUtils.castLongToInt(
                                            type.getLong(column, index))));
                        case INT64 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeLong(builder, type.getLong(column, index)));
                        case FLOAT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeFloat(builder, type.getLong(column, index)));
                        case DOUBLE ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeDouble(builder, type.getLong(column, index)));
                        case BOOLEAN ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBoolean(
                                        builder, type.getLong(column, index) != 0L));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(
                                        builder,
                                        BytesUtils.valueOf(
                                            String.valueOf(type.getLong(column, index)))));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case FLOAT ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeInt(
                                        builder,
                                        CastFunctionUtils.castFloatToInt(
                                            type.getFloat(column, index))));
                        case INT64 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeLong(
                                        builder,
                                        CastFunctionUtils.castFloatToLong(
                                            type.getFloat(column, index))));
                        case FLOAT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeFloat(builder, type.getFloat(column, index)));
                        case DOUBLE ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeDouble(builder, type.getFloat(column, index)));
                        case BOOLEAN ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBoolean(
                                        builder, type.getFloat(column, index) != 0.0f));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(
                                        builder,
                                        BytesUtils.valueOf(
                                            String.valueOf(type.getFloat(column, index)))));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case DOUBLE ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeInt(
                                        builder,
                                        CastFunctionUtils.castDoubleToInt(
                                            type.getDouble(column, index))));
                        case INT64 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeLong(
                                        builder,
                                        CastFunctionUtils.castDoubleToLong(
                                            type.getDouble(column, index))));
                        case FLOAT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeFloat(
                                        builder,
                                        CastFunctionUtils.castDoubleToFloat(
                                            type.getDouble(column, index))));
                        case DOUBLE ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeDouble(builder, type.getDouble(column, index)));
                        case BOOLEAN ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBoolean(
                                        builder, type.getDouble(column, index) != 0.0));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(
                                        builder,
                                        BytesUtils.valueOf(
                                            String.valueOf(type.getDouble(column, index)))));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case BOOLEAN ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeInt(
                                        builder, type.getBoolean(column, index) ? 1 : 0));
                        case INT64 ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeLong(
                                        builder, type.getBoolean(column, index) ? 1L : 0L));
                        case FLOAT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeFloat(
                                        builder, type.getBoolean(column, index) ? 1.0f : 0.0f));
                        case DOUBLE ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeDouble(
                                        builder, type.getBoolean(column, index) ? 1.0 : 0.0));
                        case BOOLEAN ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBoolean(
                                        builder, type.getBoolean(column, index)));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(
                                        builder,
                                        BytesUtils.valueOf(
                                            String.valueOf(type.getBoolean(column, index)))));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case TEXT ->
                  targetType ->
                      switch (targetType.getTypeEnum()) {
                        case INT32 ->
                            castBinaryColumnStrategy(
                                type,
                                targetType,
                                (value, builder) ->
                                    targetType.writeInt(builder, Integer.parseInt(value)));
                        case INT64 ->
                            castBinaryColumnStrategy(
                                type,
                                targetType,
                                (value, builder) ->
                                    targetType.writeLong(builder, Long.parseLong(value)));
                        case FLOAT ->
                            castBinaryColumnStrategy(
                                type,
                                targetType,
                                (value, builder) ->
                                    targetType.writeFloat(
                                        builder, CastFunctionUtils.castTextToFloat(value)));
                        case DOUBLE ->
                            castBinaryColumnStrategy(
                                type,
                                targetType,
                                (value, builder) ->
                                    targetType.writeDouble(
                                        builder, CastFunctionUtils.castTextToDouble(value)));
                        case BOOLEAN ->
                            castBinaryColumnStrategy(
                                type,
                                targetType,
                                (value, builder) ->
                                    targetType.writeBoolean(
                                        builder, CastFunctionUtils.castTextToBoolean(value)));
                        case TEXT ->
                            castColumnStrategy(
                                targetType,
                                (column, index, builder) ->
                                    targetType.writeBinary(builder, type.getBinary(column, index)));
                        case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                            unsupportedTargetCastStrategy(type);
                      };
              case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                  targetType -> unsupportedSourceCastStrategy(type);
            };

    private static CastColumnStrategy castColumnStrategy(
        final Type targetType, final CastColumnWriter writer) {
      return new CastColumnStrategy(() -> {}, targetType::createColumnBuilder, writer);
    }

    private static CastColumnStrategy castBinaryColumnStrategy(
        final Type sourceType, final Type targetType, final BinaryStringCastWriter writer) {
      return castColumnStrategy(
          targetType,
          (column, index, builder) ->
              writer.write(
                  sourceType.getBinary(column, index).getStringValue(TSFileConfig.STRING_CHARSET),
                  builder));
    }

    private static CastColumnStrategy unsupportedSourceCastStrategy(final Type sourceType) {
      return unsupportedCastStrategy(
          String.format("Unsupported source dataType: %s", sourceType.getTypeEnum()));
    }

    private static CastColumnStrategy unsupportedTargetCastStrategy(final Type sourceType) {
      return unsupportedCastStrategy(
          String.format("Unsupported target dataType: %s", sourceType.getTypeEnum()));
    }

    private static CastColumnStrategy unsupportedCastStrategy(final String message) {
      return new CastColumnStrategy(
          () -> {
            throw new UnsupportedOperationException(message);
          },
          ignored -> {
            throw new UnsupportedOperationException(message);
          },
          (column, index, builder) -> {
            throw new UnsupportedOperationException(message);
          });
    }

    public static final class CastColumnStrategy {
      private final Runnable validator;
      private final IntFunction<ColumnBuilder> builderFactory;
      private final CastColumnWriter writer;

      private CastColumnStrategy(
          final Runnable validator,
          final IntFunction<ColumnBuilder> builderFactory,
          final CastColumnWriter writer) {
        this.validator = validator;
        this.builderFactory = builderFactory;
        this.writer = writer;
      }

      public void validate() {
        validator.run();
      }

      public ColumnBuilder createBuilder(final int expectedEntries) {
        return builderFactory.apply(expectedEntries);
      }

      public void cast(final Column column, final int index, final ColumnBuilder builder) {
        writer.write(column, index, builder);
      }
    }

    @FunctionalInterface
    private interface CastColumnWriter {
      void write(Column column, int index, ColumnBuilder builder);
    }

    @FunctionalInterface
    private interface BinaryStringCastWriter {
      void write(String value, ColumnBuilder builder);
    }

    @FunctionalInterface
    public interface ColumnToDoubleConverter {
      double convert(Column column, int index) throws QueryProcessException;
    }

    @FunctionalInterface
    public interface ColumnValueWriter {
      void write(ColumnBuilder builder, Column column, int index);
    }

    @FunctionalInterface
    public interface RoundTransformer {
      void transform(RoundFunctionTransformer transformer, Column[] columns, ColumnBuilder builder)
          throws QueryProcessException, IOException;
    }

    @FunctionalInterface
    public interface DiffTransformer {
      void transform(DiffFunctionTransformer transformer, Column[] columns, ColumnBuilder builder)
          throws QueryProcessException;
    }

    @FunctionalInterface
    public interface NegationTransformer {
      void transform(
          ArithmeticNegationTransformer transformer, Column[] columns, ColumnBuilder builder)
          throws QueryProcessException, IOException;
    }

    @FunctionalInterface
    public interface StateWindowSplitter {
      boolean split(ValueRecorder valueRecorder, double delta, Column values, int index);
    }

    @FunctionalInterface
    public interface InTransformerSetInitializer {
      void initialize(InTransformer transformer, Set<String> values);
    }

    @FunctionalInterface
    public interface InTransformerColumnTransformer {
      void transform(InTransformer transformer, Column[] columns, ColumnBuilder builder)
          throws QueryProcessException;
    }

    static {
      CONSTANT_COLUMN_BUILDER_SERVICE.check();
      CONSTANT_FILL_SERVICE.check();
      COLUMN_BUILDER_SERVICE.check();
      VALUE_TO_DOUBLE_SERVICE.check();
      TRANSFORM_COLUMN_VALUE_WRITER_SERVICE.check();
      ROUND_TRANSFORMER_SERVICE.check();
      DIFF_TRANSFORMER_SERVICE.check();
      NEGATION_TRANSFORMER_SERVICE.check();
      STATE_WINDOW_SPLITTER_SERVICE.check();
      IN_TRANSFORMER_SET_INITIALIZER_SERVICE.check();
      IN_TRANSFORMER_COLUMN_TRANSFORMER_SERVICE.check();
      CAST_COLUMN_SERVICE.check();
    }

    private Transformation() {
      // Utility class
    }
  }

  public static final class Aggregation {

    public static final TypeService<ExtremeValueAccumulatorStrategy>
        EXTREME_VALUE_ACCUMULATOR_STRATEGY_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32, DATE ->
                      extremeValueAccumulatorStrategy(
                          (accumulator, column, index) ->
                              accumulator.updateIntResult(column.getInt(index)),
                          (accumulator, value) ->
                              accumulator.updateIntResult(((Number) value).intValue()),
                          (accumulator, column) -> accumulator.getResult().setInt(column.getInt(0)),
                          (builder, result) -> builder.writeInt(result.getInt()));
                  case INT64, TIMESTAMP ->
                      extremeValueAccumulatorStrategy(
                          (accumulator, column, index) ->
                              accumulator.updateLongResult(column.getLong(index)),
                          (accumulator, value) ->
                              accumulator.updateLongResult(((Number) value).longValue()),
                          (accumulator, column) ->
                              accumulator.getResult().setLong(column.getLong(0)),
                          (builder, result) -> builder.writeLong(result.getLong()));
                  case FLOAT ->
                      extremeValueAccumulatorStrategy(
                          (accumulator, column, index) ->
                              accumulator.updateFloatResult(column.getFloat(index)),
                          (accumulator, value) ->
                              accumulator.updateFloatResult(((Number) value).floatValue()),
                          (accumulator, column) ->
                              accumulator.getResult().setFloat(column.getFloat(0)),
                          (builder, result) -> builder.writeFloat(result.getFloat()));
                  case DOUBLE ->
                      extremeValueAccumulatorStrategy(
                          (accumulator, column, index) ->
                              accumulator.updateDoubleResult(column.getDouble(index)),
                          (accumulator, value) ->
                              accumulator.updateDoubleResult(((Number) value).doubleValue()),
                          (accumulator, column) ->
                              accumulator.getResult().setDouble(column.getDouble(0)),
                          (builder, result) -> builder.writeDouble(result.getDouble()));
                  case STRING ->
                      extremeValueAccumulatorStrategy(
                          (accumulator, column, index) ->
                              accumulator.updateBinaryResult(column.getBinary(index)),
                          (accumulator, value) -> accumulator.updateBinaryResult((Binary) value),
                          (accumulator, column) ->
                              accumulator.getResult().setBinary(column.getBinary(0)),
                          (builder, result) -> builder.writeBinary(result.getBinary()));
                  case BOOLEAN, TEXT, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                      ExtremeValueAccumulatorStrategy.unsupported();
                };

    private static ExtremeValueAccumulatorStrategy extremeValueAccumulatorStrategy(
        final ExtremeValueColumnUpdater columnUpdater,
        final ExtremeValueStatisticsUpdater statisticsUpdater,
        final ExtremeValueFinalSetter finalSetter,
        final BiConsumer<ColumnBuilder, TsPrimitiveType> resultWriter) {
      return new ExtremeValueAccumulatorStrategy(
          columnUpdater, statisticsUpdater, finalSetter, resultWriter);
    }

    public static final class ExtremeValueAccumulatorStrategy {
      private final ExtremeValueColumnUpdater columnUpdater;
      private final ExtremeValueStatisticsUpdater statisticsUpdater;
      private final ExtremeValueFinalSetter finalSetter;
      private final BiConsumer<ColumnBuilder, TsPrimitiveType> resultWriter;

      private ExtremeValueAccumulatorStrategy(
          final ExtremeValueColumnUpdater columnUpdater,
          final ExtremeValueStatisticsUpdater statisticsUpdater,
          final ExtremeValueFinalSetter finalSetter,
          final BiConsumer<ColumnBuilder, TsPrimitiveType> resultWriter) {
        this.columnUpdater = columnUpdater;
        this.statisticsUpdater = statisticsUpdater;
        this.finalSetter = finalSetter;
        this.resultWriter = resultWriter;
      }

      private static ExtremeValueAccumulatorStrategy unsupported() {
        return new ExtremeValueAccumulatorStrategy(null, null, null, null);
      }

      public boolean isSupported() {
        return columnUpdater != null;
      }

      public void addInput(
          final ExtremeValueAccumulator accumulator, final Column[] columns, final BitMap bitMap) {
        final int count = columns[0].getPositionCount();
        for (int i = 0; i < count; i++) {
          if (bitMap != null && !bitMap.isMarked(i)) {
            continue;
          }
          if (!columns[1].isNull(i)) {
            columnUpdater.update(accumulator, columns[1], i);
          }
        }
      }

      public void addIntermediate(final ExtremeValueAccumulator accumulator, final Column column) {
        columnUpdater.update(accumulator, column, 0);
      }

      public void addStatistics(
          final ExtremeValueAccumulator accumulator, final Object statisticsValue) {
        statisticsUpdater.update(accumulator, statisticsValue);
      }

      public void setFinal(final ExtremeValueAccumulator accumulator, final Column column) {
        finalSetter.set(accumulator, column);
      }

      public void writeResult(final ColumnBuilder builder, final TsPrimitiveType result) {
        resultWriter.accept(builder, result);
      }
    }

    public static final TypeService<LongSupplier> OUTPUT_COLUMN_SIZE_PER_LINE_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN, INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE ->
                  () -> type.estimateValueSize() + Byte.BYTES;
              case TEXT, BLOB, STRING, OBJECT ->
                  () -> StatisticsManager.getInstance().getMaxBinarySizeInBytes();
              case ROW, UNKNOWN, VECTOR ->
                  () -> {
                    throw new UnsupportedOperationException(
                        DataNodeQueryMessages.UNKNOWN_DATA_TYPE_2 + type.getTypeEnum());
                  };
            };

    public static final TypeService<Supplier<Accumulator>> MODE_ACCUMULATOR_PROVIDER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN -> BooleanModeAccumulator::new;
              case TEXT, BLOB, STRING -> BinaryModeAccumulator::new;
              case INT32, DATE -> IntModeAccumulator::new;
              case INT64, TIMESTAMP -> LongModeAccumulator::new;
              case FLOAT -> FloatModeAccumulator::new;
              case DOUBLE -> DoubleModeAccumulator::new;
              case OBJECT, ROW, UNKNOWN, VECTOR ->
                  () -> {
                    throw new IllegalArgumentException(
                        DataNodeQueryMessages.UNKNOWN_DATA_TYPE + type.getTypeEnum());
                  };
            };

    static {
      EXTREME_VALUE_ACCUMULATOR_STRATEGY_SERVICE.check();
      OUTPUT_COLUMN_SIZE_PER_LINE_SERVICE.check();
      MODE_ACCUMULATOR_PROVIDER_SERVICE.check();
    }

    private Aggregation() {
      // Utility class
    }

    public interface ExtremeValueAccumulator {
      TsPrimitiveType getResult();

      void updateIntResult(int value);

      void updateLongResult(long value);

      void updateFloatResult(float value);

      void updateDoubleResult(double value);

      void updateBinaryResult(Binary value);
    }

    @FunctionalInterface
    private interface ExtremeValueColumnUpdater {
      void update(ExtremeValueAccumulator accumulator, Column column, int index);
    }

    @FunctionalInterface
    private interface ExtremeValueStatisticsUpdater {
      void update(ExtremeValueAccumulator accumulator, Object value);
    }

    @FunctionalInterface
    private interface ExtremeValueFinalSetter {
      void set(ExtremeValueAccumulator accumulator, Column column);
    }
  }

  public static final class ValueConversion {

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

    public static final TypeService<ContextualValueParser> VALUE_PARSER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN -> (value, zoneId) -> CommonUtils.parseBoolean(value);
              case INT32 ->
                  (value, zoneId) -> {
                    try {
                      return Integer.parseInt(StringUtils.trim(value));
                    } catch (final NumberFormatException e) {
                      throw inconsistentValueException(value, type);
                    }
                  };
              case INT64 ->
                  (value, zoneId) -> {
                    try {
                      return Long.parseLong(StringUtils.trim(value));
                    } catch (final NumberFormatException e) {
                      throw inconsistentValueException(value, type);
                    }
                  };
              case TIMESTAMP ->
                  (value, zoneId) -> {
                    try {
                      return TypeInferenceUtils.isNumber(value)
                          ? Long.parseLong(value)
                          : DataNodeDateTimeUtils.parseDateTimeExpressionToLong(
                              StringUtils.trim(value), zoneId);
                    } catch (final Throwable e) {
                      throw new NumberFormatException(
                          String.format(
                              DataNodeMiscMessages.DATA_TYPE_NOT_CONSISTENT_WITH_CAUSE_FMT,
                              value,
                              type.getTypeEnum(),
                              e.getMessage()));
                    }
                  };
              case DATE -> (value, zoneId) -> CommonUtils.parseIntFromString(value);
              case FLOAT ->
                  (value, zoneId) -> {
                    final float result;
                    try {
                      result = Float.parseFloat(value);
                    } catch (final NumberFormatException e) {
                      throw inconsistentValueException(value, type);
                    }
                    if (Float.isInfinite(result)) {
                      throw new NumberFormatException(DataNodeMiscMessages.INPUT_FLOAT_INFINITY);
                    }
                    return result;
                  };
              case DOUBLE ->
                  (value, zoneId) -> {
                    final double result;
                    try {
                      result = Double.parseDouble(value);
                    } catch (final NumberFormatException e) {
                      throw inconsistentValueException(value, type);
                    }
                    if (Double.isInfinite(result)) {
                      throw new NumberFormatException(DataNodeMiscMessages.INPUT_DOUBLE_INFINITY);
                    }
                    return result;
                  };
              case TEXT, STRING ->
                  (value, zoneId) ->
                      new Binary(stripQuotesIfPresent(value), TSFileConfig.STRING_CHARSET);
              case BLOB ->
                  (value, zoneId) ->
                      new Binary(
                          CommonUtils.parseBlobStringToByteArray(stripQuotesIfPresent(value)));
              case OBJECT ->
                  (value, zoneId) -> {
                    throw inconsistentValueException(value, type);
                  };
              case ROW, UNKNOWN, VECTOR ->
                  (value, zoneId) -> {
                    throw new QueryProcessException(
                        DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                  };
            };

    public static final TypeService<java.util.function.Predicate<TSDataType>> AUTO_CAST_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case INT32 ->
                  targetType ->
                      targetType == TSDataType.INT64
                          || targetType == TSDataType.FLOAT
                          || targetType == TSDataType.DOUBLE;
              case INT64, FLOAT -> targetType -> targetType == TSDataType.DOUBLE;
              case BOOLEAN, DATE, TIMESTAMP, DOUBLE, TEXT, BLOB, STRING, OBJECT ->
                  targetType -> false;
              case ROW, UNKNOWN, VECTOR ->
                  targetType -> {
                    throw new IllegalArgumentException(
                        DataNodeMiscMessages.UNKNOWN_DATA_TYPE + type.getTypeEnum());
                  };
            };

    static {
      VALUE_PARSER_NO_EXCEPTION_SERVICE.check();
      VALUE_PARSER_SERVICE.check();
      AUTO_CAST_SERVICE.check();
    }

    private static NumberFormatException inconsistentValueException(
        final String value, final Type type) {
      return new NumberFormatException(
          String.format(
              DataNodeMiscMessages.DATA_TYPE_NOT_CONSISTENT_FMT, value, type.getTypeEnum()));
    }

    private static String stripQuotesIfPresent(final String value) {
      if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
          || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
        return value.length() == 1 ? value : value.substring(1, value.length() - 1);
      }
      return value;
    }

    @FunctionalInterface
    public interface ContextualValueParser {
      Object parse(String value, ZoneId zoneId) throws QueryProcessException;
    }

    private ValueConversion() {
      // Utility class
    }
  }

  public static final class StorageEngine {

    private static final Binary EMPTY_BINARY = new Binary("", StandardCharsets.UTF_8);

    public static final TypeService<ChunkMetadataStatisticsConverter>
        CHUNK_METADATA_STATISTICS_CONVERTER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, BOOLEAN ->
                      StorageEngine::convertNumericOrBooleanStatistics;
                  case DATE -> StorageEngine::convertDateStatistics;
                  case STRING -> StorageEngine::convertStringStatistics;
                  case TEXT -> StorageEngine::convertTextStatistics;
                  case BLOB -> StorageEngine::convertBlobStatistics;
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      (chunkMetadata, targetDataType, statistics) -> statistics;
                };

    public static final TypeService<BiConsumer<Object, IWALByteBufferView>>
        WAL_VALUE_WRITER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN -> (value, buffer) -> WALWriteUtils.write((Boolean) value, buffer);
                  case INT32, DATE ->
                      (value, buffer) -> WALWriteUtils.write((Integer) value, buffer);
                  case INT64, TIMESTAMP ->
                      (value, buffer) -> WALWriteUtils.write((Long) value, buffer);
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
                      writer.write(
                          time, isNull ? null : ReadWriteIOUtils.readBinary(stream), isNull);
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
                  case INT32, DATE ->
                      (valueArrays, rowCount, arraySize) -> rowCount * Integer.BYTES;
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

    public static final TypeService<BatchDataColumnWriter> BATCH_DATA_COLUMN_WRITER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  (batchData, time, column, index) ->
                      batchData.putBoolean(time, type.getBoolean(column, index));
              case INT32, DATE ->
                  (batchData, time, column, index) ->
                      batchData.putInt(time, type.getInt(column, index));
              case INT64, TIMESTAMP ->
                  (batchData, time, column, index) ->
                      batchData.putLong(time, type.getLong(column, index));
              case FLOAT ->
                  (batchData, time, column, index) ->
                      batchData.putFloat(time, type.getFloat(column, index));
              case DOUBLE ->
                  (batchData, time, column, index) ->
                      batchData.putDouble(time, type.getDouble(column, index));
              case TEXT, BLOB, STRING, OBJECT ->
                  (batchData, time, column, index) ->
                      batchData.putBinary(time, type.getBinary(column, index));
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
                  case DOUBLE ->
                      (value, stream) -> ReadWriteIOUtils.write(value.getDouble(), stream);
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

    public static final TypeService<TVListObjectWriter> TV_LIST_OBJECT_WRITER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN -> (tvList, time, value) -> tvList.putBoolean(time, (boolean) value);
              case INT32, DATE -> (tvList, time, value) -> tvList.putInt(time, (int) value);
              case INT64, TIMESTAMP -> (tvList, time, value) -> tvList.putLong(time, (long) value);
              case FLOAT -> (tvList, time, value) -> tvList.putFloat(time, (float) value);
              case DOUBLE -> (tvList, time, value) -> tvList.putDouble(time, (double) value);
              case TEXT, BLOB, STRING ->
                  (tvList, time, value) -> tvList.putBinary(time, (Binary) value);
              case OBJECT, ROW, UNKNOWN, VECTOR ->
                  throw new UnSupportedDataTypeException(
                          DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                      .setChecked(true);
            };

    private static final TVListProvider UNSUPPORTED_TV_LIST_PROVIDER =
        new TVListProvider(() -> null, stream -> null, stream -> null);

    public static final TypeService<TVListProvider> TV_LIST_PROVIDER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  new TVListProvider(
                      BooleanTVList::newList,
                      BooleanTVList::deserialize,
                      BooleanTVList::deserializeWithoutBitMap);
              case INT32 ->
                  new TVListProvider(
                      () -> IntTVList.newList(TSDataType.INT32),
                      stream -> IntTVList.deserialize(stream, TSDataType.INT32),
                      stream -> IntTVList.deserializeWithoutBitMap(stream, TSDataType.INT32));
              case DATE ->
                  new TVListProvider(
                      () -> IntTVList.newList(TSDataType.DATE),
                      stream -> IntTVList.deserialize(stream, TSDataType.DATE),
                      stream -> IntTVList.deserializeWithoutBitMap(stream, TSDataType.DATE));
              case INT64, TIMESTAMP ->
                  new TVListProvider(
                      LongTVList::newList,
                      LongTVList::deserialize,
                      LongTVList::deserializeWithoutBitMap);
              case FLOAT ->
                  new TVListProvider(
                      FloatTVList::newList,
                      FloatTVList::deserialize,
                      FloatTVList::deserializeWithoutBitMap);
              case DOUBLE ->
                  new TVListProvider(
                      DoubleTVList::newList,
                      DoubleTVList::deserialize,
                      DoubleTVList::deserializeWithoutBitMap);
              case TEXT, BLOB, STRING, OBJECT ->
                  new TVListProvider(
                      BinaryTVList::newList,
                      BinaryTVList::deserialize,
                      BinaryTVList::deserializeWithoutBitMap);
              case ROW, UNKNOWN, VECTOR -> UNSUPPORTED_TV_LIST_PROVIDER;
            };

    public static final TypeService<TVListChunkWriter> TV_LIST_CHUNK_WRITER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  (writer, time, tvList, index) -> {
                    writer.write(time, tvList.getBoolean(index));
                    return 8L + 1L;
                  };
              case INT32, DATE ->
                  (writer, time, tvList, index) -> {
                    writer.write(time, tvList.getInt(index));
                    return 8L + 4L;
                  };
              case INT64, TIMESTAMP ->
                  (writer, time, tvList, index) -> {
                    writer.write(time, tvList.getLong(index));
                    return 8L + 8L;
                  };
              case FLOAT ->
                  (writer, time, tvList, index) -> {
                    writer.write(time, tvList.getFloat(index));
                    return 8L + 4L;
                  };
              case DOUBLE ->
                  (writer, time, tvList, index) -> {
                    writer.write(time, tvList.getDouble(index));
                    return 8L + 8L;
                  };
              case TEXT, BLOB, STRING, OBJECT ->
                  (writer, time, tvList, index) -> {
                    Binary value = tvList.getBinary(index);
                    writer.write(time, value);
                    return 8L + MemUtils.getBinarySize(value);
                  };
              case ROW, UNKNOWN, VECTOR ->
                  throw new UnSupportedDataTypeException(
                          DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                      .setChecked(true);
            };

    public static final TypeService<TVListBatchWriter> TV_LIST_BATCH_WRITER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    boolean value = tvList.getBoolean(index);
                    if (filter != null && !filter.satisfyBoolean(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeBoolean(value);
                    builder.declarePosition();
                    return true;
                  };
              case INT32, DATE ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    int value = tvList.getInt(index);
                    if (filter != null && !filter.satisfyInteger(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeInt(value);
                    builder.declarePosition();
                    return true;
                  };
              case INT64, TIMESTAMP ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    long value = tvList.getLong(index);
                    if (filter != null && !filter.satisfyLong(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeLong(value);
                    builder.declarePosition();
                    return true;
                  };
              case FLOAT ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    float value = tvList.getFloat(index);
                    if (!Float.isNaN(value)
                        && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
                      value = MathUtils.roundWithGivenPrecision(value, floatPrecision);
                    }
                    if (filter != null && !filter.satisfyFloat(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeFloat(value);
                    builder.declarePosition();
                    return true;
                  };
              case DOUBLE ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    double value = tvList.getDouble(index);
                    if (!Double.isNaN(value)
                        && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
                      value = MathUtils.roundWithGivenPrecision(value, floatPrecision);
                    }
                    if (filter != null && !filter.satisfyDouble(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeDouble(value);
                    builder.declarePosition();
                    return true;
                  };
              case TEXT, BLOB, STRING, OBJECT ->
                  (tvList, index, time, filter, builder, floatPrecision, encoding, pagination) -> {
                    Binary value = tvList.getBinary(index);
                    if (filter != null && !filter.satisfyBinary(time, value)) {
                      return false;
                    }
                    if (consumeOffset(pagination)) {
                      return true;
                    }
                    consumeLimit(pagination);
                    builder.getTimeColumnBuilder().writeLong(time);
                    builder.getColumnBuilder(0).writeBinary(value);
                    builder.declarePosition();
                    return true;
                  };
              case ROW, UNKNOWN, VECTOR ->
                  throw new UnSupportedDataTypeException(
                          DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                      .setChecked(true);
            };

    private static boolean consumeOffset(PaginationController paginationController) {
      if (paginationController != null && paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
        return true;
      }
      return false;
    }

    private static void consumeLimit(PaginationController paginationController) {
      if (paginationController != null) {
        paginationController.consumeLimit();
      }
    }

    public static final TypeService<AlignedTVListChunkWriter> ALIGNED_TV_LIST_CHUNK_WRITER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? false : tvList.getBooleanByValueIndex(rowIndex, columnIndex),
                          isNull);
              case INT32, DATE ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? 0 : tvList.getIntByValueIndex(rowIndex, columnIndex),
                          isNull);
              case INT64, TIMESTAMP ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? 0L : tvList.getLongByValueIndex(rowIndex, columnIndex),
                          isNull);
              case FLOAT ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? 0F : tvList.getFloatByValueIndex(rowIndex, columnIndex),
                          isNull);
              case DOUBLE ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? 0D : tvList.getDoubleByValueIndex(rowIndex, columnIndex),
                          isNull);
              case TEXT, BLOB, STRING, OBJECT ->
                  (writer, time, tvList, rowIndex, columnIndex, isNull) ->
                      writer.write(
                          time,
                          isNull ? null : tvList.getBinaryByValueIndex(rowIndex, columnIndex),
                          isNull);
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
                  throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                      .setChecked(true);
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
                  throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                      .setChecked(true);
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
                  throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                      .setChecked(true);
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
                  throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                      .setChecked(true);
            };

    public static final TypeService<BiFunction<ByteBuffer, Integer, Object>>
        RAW_ARRAY_BYTE_BUFFER_DESERIALIZER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN,
                      INT32,
                      INT64,
                      TIMESTAMP,
                      FLOAT,
                      DOUBLE,
                      TEXT,
                      BLOB,
                      STRING,
                      OBJECT ->
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
                  case BOOLEAN,
                      INT32,
                      INT64,
                      TIMESTAMP,
                      FLOAT,
                      DOUBLE,
                      TEXT,
                      BLOB,
                      STRING,
                      OBJECT ->
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
                  throw new UnSupportedDataTypeException(type.getTypeEnum().name())
                      .setChecked(true);
            };

    static {
      CHUNK_METADATA_STATISTICS_CONVERTER_SERVICE.check();
      TV_LIST_ARRAY_WRITER_SERVICE.check();
      TV_LIST_OBJECT_WRITER_SERVICE.check();
      TV_LIST_PROVIDER_SERVICE.check();
      TV_LIST_CHUNK_WRITER_SERVICE.check();
      TV_LIST_BATCH_WRITER_SERVICE.check();
      ALIGNED_TV_LIST_CHUNK_WRITER_SERVICE.check();
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
      BATCH_DATA_COLUMN_WRITER_SERVICE.check();
      OBJECT_VALUE_SERIALIZER_SERVICE.check();
      TS_PRIMITIVE_VALUE_SERIALIZER_SERVICE.check();
      EMPTY_TS_PRIMITIVE_TYPE_FACTORY_SERVICE.check();
      DEFAULT_ENCODING_SERVICE.check();
      TABLET_COLUMN_DECODER_SERVICE.check();
      DECODED_ARRAY_VALUE_READER_SERVICE.check();
      DECODED_CHUNK_WRITER_SERVICE.check();
    }

    private static Statistics<?> convertNumericOrBooleanStatistics(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
      if (targetDataType == TSDataType.STRING) {
        Binary[] binaryValues = new Binary[4];
        binaryValues[0] =
            new Binary(
                chunkMetadata.getStatistics().getFirstValue().toString(), StandardCharsets.UTF_8);
        binaryValues[1] =
            new Binary(
                chunkMetadata.getStatistics().getLastValue().toString(), StandardCharsets.UTF_8);
        if (chunkMetadata.getDataType() == TSDataType.BOOLEAN) {
          binaryValues[2] = new Binary(Boolean.FALSE.toString(), StandardCharsets.UTF_8);
          binaryValues[3] = new Binary(Boolean.TRUE.toString(), StandardCharsets.UTF_8);
        } else {
          binaryValues[2] =
              new Binary(
                  chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8);
          binaryValues[3] =
              new Binary(
                  chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8);
        }
        updateStatistics(statistics, chunkMetadata, binaryValues);
        return statistics;
      }
      if (targetDataType == TSDataType.TEXT) {
        Binary[] binaryValues = new Binary[2];
        if (chunkMetadata.getDataType() == TSDataType.BOOLEAN) {
          binaryValues[0] = new Binary(Boolean.FALSE.toString(), StandardCharsets.UTF_8);
          binaryValues[1] = new Binary(Boolean.TRUE.toString(), StandardCharsets.UTF_8);
        } else {
          binaryValues[0] =
              new Binary(
                  chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8);
          binaryValues[1] =
              new Binary(
                  chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8);
        }
        updateStatistics(statistics, chunkMetadata, binaryValues);
        return statistics;
      }
      return chunkMetadata.getStatistics();
    }

    private static Statistics<?> convertDateStatistics(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
      if (targetDataType != TSDataType.STRING && targetDataType != TSDataType.TEXT) {
        return statistics;
      }
      int valueCount = targetDataType == TSDataType.STRING ? 4 : 2;
      Binary[] binaryValues = new Binary[valueCount];
      binaryValues[0] = toDateBinary(chunkMetadata.getStatistics().getFirstValue());
      binaryValues[1] = toDateBinary(chunkMetadata.getStatistics().getLastValue());
      if (targetDataType == TSDataType.STRING) {
        binaryValues[2] = toDateBinary(chunkMetadata.getStatistics().getMinValue());
        binaryValues[3] = toDateBinary(chunkMetadata.getStatistics().getMaxValue());
      }
      updateStatistics(statistics, chunkMetadata, binaryValues);
      return statistics;
    }

    private static Statistics<?> convertStringStatistics(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
      if (targetDataType == TSDataType.TEXT) {
        Binary[] binaryValues = {
          new Binary(
              chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8),
          new Binary(chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8)
        };
        updateStatistics(statistics, chunkMetadata, binaryValues);
        return statistics;
      }
      if (targetDataType == TSDataType.BLOB) {
        statistics.update(
            chunkMetadata.getStatistics().getStartTime(),
            new Binary(
                chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8));
        statistics.update(
            chunkMetadata.getStatistics().getEndTime(),
            new Binary(
                chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8));
        return statistics;
      }
      return chunkMetadata.getStatistics();
    }

    private static Statistics<?> convertTextStatistics(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
      if (targetDataType == TSDataType.STRING) {
        Binary[] binaryValues = {
          (Binary) chunkMetadata.getStatistics().getFirstValue(),
          (Binary) chunkMetadata.getStatistics().getLastValue()
        };
        updateStatistics(statistics, chunkMetadata, binaryValues);
        return statistics;
      }
      if (targetDataType == TSDataType.BLOB) {
        statistics.update(chunkMetadata.getStatistics().getStartTime(), EMPTY_BINARY);
        statistics.update(chunkMetadata.getStatistics().getEndTime(), EMPTY_BINARY);
        return statistics;
      }
      return chunkMetadata.getStatistics();
    }

    private static Statistics<?> convertBlobStatistics(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
      if (targetDataType == TSDataType.STRING || targetDataType == TSDataType.TEXT) {
        updateStatistics(statistics, chunkMetadata, new Binary[] {EMPTY_BINARY, EMPTY_BINARY});
        return statistics;
      }
      return chunkMetadata.getStatistics();
    }

    private static Binary toDateBinary(Object value) {
      return new Binary(TSDataType.getDateStringValue((Integer) value), StandardCharsets.UTF_8);
    }

    private static void updateStatistics(
        Statistics<?> statistics, IChunkMetadata chunkMetadata, Binary[] binaryValues) {
      long[] longValues = new long[binaryValues.length];
      longValues[0] = chunkMetadata.getStatistics().getStartTime();
      longValues[1] = chunkMetadata.getStatistics().getEndTime();
      Arrays.fill(longValues, 2, longValues.length, longValues[1]);
      statistics.update(longValues, binaryValues, binaryValues.length);
    }

    private StorageEngine() {
      // Utility class
    }
  }

  public static final class Predicate {

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
                          new Binary(
                              ((StringLiteral) value).getValue(), TSFileConfig.STRING_CHARSET);
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
                              DataNodeQueryMessages
                                  .UNSUPPORTED_SCALAR_SUBQUERY_RESULT_DATA_TYPE_FMT,
                              type.getTypeEnum()));
                };

    private Predicate() {
      // Utility class
    }
  }

  public static final class Pipe {

    public static final TypeService<SameTypeNumericOperatorStrategy>
        SAME_TYPE_NUMERIC_OPERATOR_STRATEGY_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32 ->
                      sameTypeNumericOperatorStrategy(
                          TSDataType.INT32,
                          AbstractSameTypeNumericOperator::getIntValue,
                          (operator, stream) ->
                              ReadWriteIOUtils.write(operator.getIntValue(), stream),
                          (operator, buffer) ->
                              operator.setIntValue(ReadWriteIOUtils.readInt(buffer)));
                  case INT64 ->
                      sameTypeNumericOperatorStrategy(
                          TSDataType.INT64,
                          AbstractSameTypeNumericOperator::getLongValue,
                          (operator, stream) ->
                              ReadWriteIOUtils.write(operator.getLongValue(), stream),
                          (operator, buffer) ->
                              operator.setLongValue(ReadWriteIOUtils.readLong(buffer)));
                  case FLOAT ->
                      sameTypeNumericOperatorStrategy(
                          TSDataType.FLOAT,
                          AbstractSameTypeNumericOperator::getFloatValue,
                          (operator, stream) ->
                              ReadWriteIOUtils.write(operator.getFloatValue(), stream),
                          (operator, buffer) ->
                              operator.setFloatValue(ReadWriteIOUtils.readFloat(buffer)));
                  case DOUBLE ->
                      sameTypeNumericOperatorStrategy(
                          TSDataType.DOUBLE,
                          AbstractSameTypeNumericOperator::getDoubleValue,
                          (operator, stream) ->
                              ReadWriteIOUtils.write(operator.getDoubleValue(), stream),
                          (operator, buffer) ->
                              operator.setDoubleValue(ReadWriteIOUtils.readDouble(buffer)));
                  case BOOLEAN, DATE, TIMESTAMP, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                      unsupportedSameTypeNumericOperatorStrategy(
                          TSDataType.valueOf(type.getTypeEnum().name()));
                };

    private static SameTypeNumericOperatorStrategy sameTypeNumericOperatorStrategy(
        final TSDataType dataType,
        final Function<AbstractSameTypeNumericOperator, Object> valueGetter,
        final SameTypeNumericOperatorSerializer serializer,
        final SameTypeNumericOperatorDeserializer deserializer) {
      return new SameTypeNumericOperatorStrategy(dataType, valueGetter, serializer, deserializer);
    }

    private static SameTypeNumericOperatorStrategy unsupportedSameTypeNumericOperatorStrategy(
        final TSDataType dataType) {
      return new SameTypeNumericOperatorStrategy(
          dataType,
          operator -> null,
          (operator, stream) -> {
            throw new IOException(
                String.format(DataNodePipeMessages.UNSUPPORTED_OUTPUT_DATATYPE_FMT, dataType));
          },
          (operator, buffer) -> {
            throw new IOException(
                String.format(DataNodePipeMessages.UNSUPPORTED_OUTPUT_DATATYPE_FMT, dataType));
          });
    }

    public static final class SameTypeNumericOperatorStrategy {
      private final TSDataType dataType;
      private final Function<AbstractSameTypeNumericOperator, Object> valueGetter;
      private final SameTypeNumericOperatorSerializer serializer;
      private final SameTypeNumericOperatorDeserializer deserializer;

      private SameTypeNumericOperatorStrategy(
          final TSDataType dataType,
          final Function<AbstractSameTypeNumericOperator, Object> valueGetter,
          final SameTypeNumericOperatorSerializer serializer,
          final SameTypeNumericOperatorDeserializer deserializer) {
        this.dataType = dataType;
        this.valueGetter = valueGetter;
        this.serializer = serializer;
        this.deserializer = deserializer;
      }

      public Pair<TSDataType, Object> getResult(final AbstractSameTypeNumericOperator operator) {
        final Object value = valueGetter.apply(operator);
        return value == null ? null : new Pair<>(dataType, value);
      }

      public void serialize(
          final AbstractSameTypeNumericOperator operator, final DataOutputStream stream)
          throws IOException {
        serializer.serialize(operator, stream);
      }

      public void deserialize(
          final AbstractSameTypeNumericOperator operator, final ByteBuffer buffer)
          throws IOException {
        deserializer.deserialize(operator, buffer);
      }
    }

    @FunctionalInterface
    private interface SameTypeNumericOperatorSerializer {
      void serialize(AbstractSameTypeNumericOperator operator, DataOutputStream stream)
          throws IOException;
    }

    @FunctionalInterface
    private interface SameTypeNumericOperatorDeserializer {
      void deserialize(AbstractSameTypeNumericOperator operator, ByteBuffer buffer)
          throws IOException;
    }

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

    public static final TypeService<OpcDaTabletValueSetter> OPC_DA_TABLET_VALUE_SETTER_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN ->
                  (value, column, rowIndex) -> {
                    value.setValue(
                        Variant.VT_BOOL, new OaIdl.VARIANT_BOOL(((boolean[]) column)[rowIndex]));
                    return null;
                  };
              case INT32 ->
                  (value, column, rowIndex) -> {
                    value.setValue(Variant.VT_I4, new WinDef.LONG(((int[]) column)[rowIndex]));
                    return null;
                  };
              case DATE ->
                  (value, column, rowIndex) -> {
                    value.setValue(
                        Variant.VT_DATE,
                        new OaIdl.DATE(Date.valueOf(((LocalDate[]) column)[rowIndex])));
                    return null;
                  };
              case INT64 ->
                  (value, column, rowIndex) -> {
                    value.setValue(Variant.VT_I8, new WinDef.LONGLONG(((long[]) column)[rowIndex]));
                    return null;
                  };
              case TIMESTAMP ->
                  (value, column, rowIndex) -> {
                    value.setValue(
                        Variant.VT_DATE,
                        new OaIdl.DATE(new java.util.Date(((long[]) column)[rowIndex])));
                    return null;
                  };
              case FLOAT ->
                  (value, column, rowIndex) -> {
                    value.setValue(Variant.VT_R4, ((float[]) column)[rowIndex]);
                    return null;
                  };
              case DOUBLE ->
                  (value, column, rowIndex) -> {
                    value.setValue(Variant.VT_R8, ((double[]) column)[rowIndex]);
                    return null;
                  };
              case TEXT, STRING, BLOB, OBJECT ->
                  (value, column, rowIndex) -> {
                    final WTypes.BSTR bstr =
                        OleAuto.INSTANCE.SysAllocString(((Binary[]) column)[rowIndex].toString());
                    value.setValue(Variant.VT_BSTR, bstr);
                    return bstr;
                  };
              case ROW, UNKNOWN, VECTOR ->
                  (value, column, rowIndex) -> {
                    throw new UnSupportedDataTypeException(
                        DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum());
                  };
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

    public static final TypeService<ToIntFunction<Object>>
        CUSTOMIZED_INTERMEDIATE_RESULT_TO_INT_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32 -> value -> (int) value;
                  case DATE -> value -> DateUtils.parseDateExpressionToInt((LocalDate) value);
                  case INT64, TIMESTAMP -> value -> (int) (long) value;
                  case FLOAT -> value -> (int) (float) value;
                  case DOUBLE -> value -> (int) (double) value;
                  case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                      value -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                "The type %s cannot be casted to int.", type.getTypeEnum()));
                      };
                };

    public static final TypeService<ToLongFunction<Object>>
        CUSTOMIZED_INTERMEDIATE_RESULT_TO_LONG_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32 -> value -> (int) value;
                  case DATE -> value -> DateUtils.parseDateExpressionToInt((LocalDate) value);
                  case INT64, TIMESTAMP -> value -> (long) value;
                  case FLOAT -> value -> (long) (float) value;
                  case DOUBLE -> value -> (long) (double) value;
                  case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                      value -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                "The type %s cannot be casted to long.", type.getTypeEnum()));
                      };
                };

    public static final TypeService<Function<Object, Float>>
        CUSTOMIZED_INTERMEDIATE_RESULT_TO_FLOAT_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32 -> value -> (float) (int) value;
                  case DATE ->
                      value -> (float) DateUtils.parseDateExpressionToInt((LocalDate) value);
                  case INT64, TIMESTAMP -> value -> (float) (long) value;
                  case FLOAT -> value -> (float) value;
                  case DOUBLE -> value -> (float) (double) value;
                  case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                      value -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                "The type %s cannot be casted to float.", type.getTypeEnum()));
                      };
                };

    public static final TypeService<ToDoubleFunction<Object>>
        CUSTOMIZED_INTERMEDIATE_RESULT_TO_DOUBLE_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case INT32 -> value -> (int) value;
                  case DATE -> value -> DateUtils.parseDateExpressionToInt((LocalDate) value);
                  case INT64, TIMESTAMP -> value -> (long) value;
                  case FLOAT -> value -> (float) value;
                  case DOUBLE -> value -> (double) value;
                  case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                      value -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                "The type %s cannot be casted to double.", type.getTypeEnum()));
                      };
                };

    public static final TypeService<Function<Object, String>>
        CUSTOMIZED_INTERMEDIATE_RESULT_TO_STRING_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN -> value -> Boolean.toString((boolean) value);
                  case INT32 -> value -> Integer.toString((int) value);
                  case DATE -> value -> ((LocalDate) value).toString();
                  case INT64 -> value -> Long.toString((long) value);
                  case TIMESTAMP ->
                      value ->
                          RpcUtils.formatDatetime(
                              RpcUtils.DEFAULT_TIME_FORMAT,
                              CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
                              (long) value,
                              ZoneId.systemDefault());
                  case FLOAT -> value -> Float.toString((float) value);
                  case DOUBLE -> value -> Double.toString((double) value);
                  case TEXT, STRING -> value -> (String) value;
                  case BLOB ->
                      value ->
                          BytesUtils.parseBlobByteArrayToString(
                              ((org.apache.iotdb.pipe.api.type.Binary) value).getValues());
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      value -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                "The type %s cannot be casted to string.", type.getTypeEnum()));
                      };
                };

    public static final TypeService<IntFunction<Object>> AGGREGATE_TABLET_COLUMN_ALLOCATOR_SERVICE =
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
                  size -> {
                    throw new UnsupportedOperationException(
                        String.format(
                            DataNodePipeMessages.UNSUPPORTED_OUTPUT_DATATYPE_FMT,
                            type.getTypeEnum()));
                  };
            };

    public static final TypeService<AggregateTabletColumnValueWriter>
        AGGREGATE_TABLET_COLUMN_VALUE_WRITER_SERVICE =
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
                      (column, rowIndex, value) -> type.addValue(rowIndex, value, column);
                  case ROW, UNKNOWN, VECTOR ->
                      (column, rowIndex, value) -> {
                        throw new UnsupportedOperationException(
                            String.format(
                                DataNodePipeMessages.UNSUPPORTED_OUTPUT_DATATYPE_FMT,
                                type.getTypeEnum()));
                      };
                };

    public static final TypeService<TabletObjectValueGetter>
        OPC_UA_TABLET_OBJECT_VALUE_GETTER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN -> (column, rowIndex) -> ((boolean[]) column)[rowIndex];
                  case INT32 -> (column, rowIndex) -> ((int[]) column)[rowIndex];
                  case DATE ->
                      (column, rowIndex) ->
                          new DateTime(Date.valueOf(((LocalDate[]) column)[rowIndex]));
                  case INT64 -> (column, rowIndex) -> ((long[]) column)[rowIndex];
                  case TIMESTAMP ->
                      (column, rowIndex) ->
                          new DateTime(
                              TimestampPrecisionUtils.currPrecision.toNanos(
                                          ((long[]) column)[rowIndex])
                                      / 100L
                                  + 116444736000000000L);
                  case FLOAT -> (column, rowIndex) -> ((float[]) column)[rowIndex];
                  case DOUBLE -> (column, rowIndex) -> ((double[]) column)[rowIndex];
                  case TEXT, BLOB, STRING ->
                      (column, rowIndex) -> ((Binary[]) column)[rowIndex].toString();
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      (column, rowIndex) -> {
                        throw new UnSupportedDataTypeException(
                            DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum());
                      };
                };

    public static final TypeService<Supplier<NodeId>> OPC_UA_DATA_TYPE_SERVICE =
        type ->
            switch (type.getTypeEnum()) {
              case BOOLEAN -> () -> Identifiers.Boolean;
              case INT32 -> () -> Identifiers.Int32;
              case DATE, TIMESTAMP -> () -> Identifiers.DateTime;
              case INT64 -> () -> Identifiers.Int64;
              case FLOAT -> () -> Identifiers.Float;
              case DOUBLE -> () -> Identifiers.Double;
              case TEXT, BLOB, STRING -> () -> Identifiers.String;
              case OBJECT, ROW, UNKNOWN, VECTOR ->
                  () -> {
                    throw new PipeRuntimeNonCriticalException(
                        DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                  };
            };

    @FunctionalInterface
    public interface TabletObjectValueGetter {
      Object get(Object column, int rowIndex);
    }

    @FunctionalInterface
    public interface AggregateTabletColumnValueWriter {
      void write(Object column, int rowIndex, Object value);
    }

    public static final TypeService<Function<Boolean, Type>>
        PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN,
                      INT32,
                      INT64,
                      TIMESTAMP,
                      FLOAT,
                      DOUBLE,
                      TEXT,
                      BLOB,
                      OBJECT,
                      STRING ->
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

    public static final TypeService<org.apache.iotdb.pipe.api.type.Type>
        PIPE_DATA_TYPE_TRANSFORMER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN -> org.apache.iotdb.pipe.api.type.Type.BOOLEAN;
                  case INT32 -> org.apache.iotdb.pipe.api.type.Type.INT32;
                  case INT64 -> org.apache.iotdb.pipe.api.type.Type.INT64;
                  case FLOAT -> org.apache.iotdb.pipe.api.type.Type.FLOAT;
                  case DOUBLE -> org.apache.iotdb.pipe.api.type.Type.DOUBLE;
                  case TEXT -> org.apache.iotdb.pipe.api.type.Type.TEXT;
                  case TIMESTAMP -> org.apache.iotdb.pipe.api.type.Type.TIMESTAMP;
                  case DATE -> org.apache.iotdb.pipe.api.type.Type.DATE;
                  case BLOB -> org.apache.iotdb.pipe.api.type.Type.BLOB;
                  case STRING -> org.apache.iotdb.pipe.api.type.Type.STRING;
                  case OBJECT ->
                      throw new IllegalArgumentException(
                          DataNodePipeMessages.INVALID_INPUT + TSDataType.OBJECT.getType());
                  case UNKNOWN ->
                      throw new IllegalArgumentException(
                          DataNodePipeMessages.INVALID_INPUT + TSDataType.UNKNOWN.getType());
                  case VECTOR ->
                      throw new IllegalArgumentException(
                          DataNodePipeMessages.INVALID_INPUT + TSDataType.VECTOR.getType());
                  case ROW ->
                      throw new IllegalArgumentException(
                          DataNodePipeMessages.INVALID_INPUT + type.getTypeEnum());
                };

    public static final TypeService<TsPrimitiveTabletValueWriter>
        PIPE_TS_PRIMITIVE_TABLET_VALUE_WRITER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN, INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE -> type::write;
                  case TEXT, BLOB, STRING ->
                      (value, column, index) -> {
                        type.write(value, column, index);
                        final Binary[] binaryColumn = (Binary[]) column;
                        binaryColumn[index] = normalizeBinaryValue(binaryColumn[index]);
                      };
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      (value, column, index) -> {
                        throw new UnSupportedDataTypeException(
                            DataNodePipeMessages.UNSUPPORTED + type.getTypeEnum());
                      };
                };

    public static final TypeService<BatchDataTabletValueWriter>
        PIPE_BATCH_DATA_TABLET_VALUE_WRITER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN, INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE -> type::write;
                  case TEXT, BLOB, STRING ->
                      (value, column, index) -> {
                        type.write(value, column, index);
                        final Binary[] binaryColumn = (Binary[]) column;
                        binaryColumn[index] = normalizeBinaryValue(binaryColumn[index]);
                      };
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      (value, column, index) -> {
                        throw new UnSupportedDataTypeException(
                            DataNodePipeMessages.UNSUPPORTED + type.getTypeEnum());
                      };
                };

    private static Binary normalizeBinaryValue(final Binary value) {
      return Objects.isNull(value) || Objects.isNull(value.getValues())
          ? Binary.EMPTY_VALUE
          : value;
    }

    public static final TypeService<TabletValueColumnFilter>
        PIPE_TABLET_VALUE_COLUMN_FILTER_SERVICE =
            type ->
                switch (type.getTypeEnum()) {
                  case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE ->
                      (originValueColumn,
                          rowIndexList,
                          isSingleOriginValueColumn,
                          originNullValueColumnBitmap,
                          nullValueColumnBitmap) ->
                          filterPrimitiveValueColumn(
                              type,
                              originValueColumn,
                              rowIndexList,
                              isSingleOriginValueColumn,
                              originNullValueColumnBitmap,
                              nullValueColumnBitmap);
                  case DATE -> Pipe::filterDateValueColumn;
                  case TEXT, BLOB, STRING -> Pipe::filterBinaryValueColumn;
                  case OBJECT, ROW, UNKNOWN, VECTOR ->
                      (originValueColumn,
                          rowIndexList,
                          isSingleOriginValueColumn,
                          originNullValueColumnBitmap,
                          nullValueColumnBitmap) -> {
                        throw new UnSupportedDataTypeException(
                            String.format("Data type %s is not supported.", type.getTypeEnum()));
                      };
                };

    private static Object filterPrimitiveValueColumn(
        final Type type,
        final Object originValueColumn,
        final List<Integer> rowIndexList,
        final boolean isSingleOriginValueColumn,
        final BitMap originNullValueColumnBitmap,
        final BitMap nullValueColumnBitmap) {
      final Object originValueColumns;
      if (isSingleOriginValueColumn) {
        originValueColumns = type.createArray(1);
        type.addValue(0, originValueColumn, originValueColumns);
      } else {
        originValueColumns = originValueColumn;
      }
      final Object valueColumns = type.createArray(rowIndexList.size());
      for (int i = 0; i < rowIndexList.size(); ++i) {
        final int originRowIndex = rowIndexList.get(i);
        if (isNullValue(originNullValueColumnBitmap, originRowIndex)) {
          nullValueColumnBitmap.mark(i);
        } else {
          type.copyArrayElement(originValueColumns, originRowIndex, valueColumns, i);
        }
      }
      return valueColumns;
    }

    private static LocalDate[] filterDateValueColumn(
        final Object originValueColumn,
        final List<Integer> rowIndexList,
        final boolean isSingleOriginValueColumn,
        final BitMap originNullValueColumnBitmap,
        final BitMap nullValueColumnBitmap) {
      final LocalDate[] valueColumns = new LocalDate[rowIndexList.size()];
      final boolean isLocalDateColumn =
          isSingleOriginValueColumn
              ? originValueColumn instanceof LocalDate
              : originValueColumn instanceof LocalDate[];
      final LocalDate[] dateValueColumns =
          isLocalDateColumn
              ? (isSingleOriginValueColumn
                  ? new LocalDate[] {(LocalDate) originValueColumn}
                  : (LocalDate[]) originValueColumn)
              : null;
      final int[] intValueColumns =
          isLocalDateColumn
              ? null
              : (isSingleOriginValueColumn
                  ? new int[] {(int) originValueColumn}
                  : (int[]) originValueColumn);
      for (int i = 0; i < rowIndexList.size(); ++i) {
        final int originRowIndex = rowIndexList.get(i);
        if (isNullValue(originNullValueColumnBitmap, originRowIndex)) {
          valueColumns[i] = EMPTY_LOCAL_DATE;
          nullValueColumnBitmap.mark(i);
        } else {
          valueColumns[i] =
              isLocalDateColumn
                  ? dateValueColumns[originRowIndex]
                  : DateUtils.parseIntToLocalDate(intValueColumns[originRowIndex]);
        }
      }
      return valueColumns;
    }

    private static Binary[] filterBinaryValueColumn(
        final Object originValueColumn,
        final List<Integer> rowIndexList,
        final boolean isSingleOriginValueColumn,
        final BitMap originNullValueColumnBitmap,
        final BitMap nullValueColumnBitmap) {
      final Binary[] binaryValueColumns =
          isSingleOriginValueColumn
              ? new Binary[] {(Binary) originValueColumn}
              : (Binary[]) originValueColumn;
      final Binary[] valueColumns = new Binary[rowIndexList.size()];
      for (int i = 0; i < rowIndexList.size(); ++i) {
        final int originRowIndex = rowIndexList.get(i);
        final Binary value = binaryValueColumns[originRowIndex];
        if (Objects.isNull(value)
            || Objects.isNull(value.getValues())
            || isNullValue(originNullValueColumnBitmap, originRowIndex)) {
          valueColumns[i] = Binary.EMPTY_VALUE;
          nullValueColumnBitmap.mark(i);
        } else {
          valueColumns[i] = new Binary(value.getValues());
        }
      }
      return valueColumns;
    }

    private static boolean isNullValue(final BitMap bitMap, final int rowIndex) {
      return Objects.nonNull(bitMap) && bitMap.isMarked(rowIndex);
    }

    static {
      SAME_TYPE_NUMERIC_OPERATOR_STRATEGY_SERVICE.check();
      OPC_DA_TABLET_VALUE_SETTER_SERVICE.check();
      OPC_UA_VALUE_STRINGIFIER_SERVICE.check();
      CUSTOMIZED_INTERMEDIATE_RESULT_TO_INT_SERVICE.check();
      CUSTOMIZED_INTERMEDIATE_RESULT_TO_LONG_SERVICE.check();
      CUSTOMIZED_INTERMEDIATE_RESULT_TO_FLOAT_SERVICE.check();
      CUSTOMIZED_INTERMEDIATE_RESULT_TO_DOUBLE_SERVICE.check();
      CUSTOMIZED_INTERMEDIATE_RESULT_TO_STRING_SERVICE.check();
      AGGREGATE_TABLET_COLUMN_ALLOCATOR_SERVICE.check();
      AGGREGATE_TABLET_COLUMN_VALUE_WRITER_SERVICE.check();
      OPC_UA_TABLET_OBJECT_VALUE_GETTER_SERVICE.check();
      OPC_UA_DATA_TYPE_SERVICE.check();
      PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE.check();
      PIPE_DATA_TYPE_TRANSFORMER_SERVICE.check();
      PIPE_TS_PRIMITIVE_TABLET_VALUE_WRITER_SERVICE.check();
      PIPE_BATCH_DATA_TABLET_VALUE_WRITER_SERVICE.check();
      PIPE_TABLET_VALUE_COLUMN_FILTER_SERVICE.check();
    }

    private Pipe() {
      // Utility class
    }
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
  public interface TabletValueColumnFilter {
    Object filter(
        Object originValueColumn,
        List<Integer> rowIndexList,
        boolean isSingleOriginValueColumn,
        BitMap originNullValueColumnBitmap,
        BitMap nullValueColumnBitmap);
  }

  @FunctionalInterface
  public interface TsPrimitiveTabletValueWriter {
    void write(TsPrimitiveType value, Object column, int index);
  }

  @FunctionalInterface
  public interface BatchDataTabletValueWriter {
    void write(BatchData value, Object column, int index);
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
  public interface TVListObjectWriter {
    void write(TVList tvList, long time, Object value);
  }

  public static class TVListProvider {
    private final Supplier<TVList> factory;
    private final TVListDeserializer deserializer;
    private final TVListDeserializer noBitmapDeserializer;

    private TVListProvider(
        Supplier<TVList> factory,
        TVListDeserializer deserializer,
        TVListDeserializer noBitmapDeserializer) {
      this.factory = factory;
      this.deserializer = deserializer;
      this.noBitmapDeserializer = noBitmapDeserializer;
    }

    public TVList newList() {
      return factory.get();
    }

    public TVList deserialize(DataInputStream stream) throws IOException {
      return deserializer.deserialize(stream);
    }

    public TVList deserializeWithoutBitMap(DataInputStream stream) throws IOException {
      return noBitmapDeserializer.deserialize(stream);
    }
  }

  @FunctionalInterface
  private interface TVListDeserializer {
    TVList deserialize(DataInputStream stream) throws IOException;
  }

  @FunctionalInterface
  public interface TVListChunkWriter {
    long write(ChunkWriterImpl writer, long time, TVList tvList, int index);
  }

  @FunctionalInterface
  public interface TVListBatchWriter {
    boolean write(
        TVList tvList,
        int index,
        long time,
        Filter filter,
        TsBlockBuilder builder,
        int floatPrecision,
        TSEncoding encoding,
        PaginationController paginationController);
  }

  @FunctionalInterface
  public interface AlignedTVListChunkWriter {
    void write(
        ValueChunkWriter writer,
        long time,
        AlignedTVList tvList,
        int rowIndex,
        int columnIndex,
        boolean isNull);
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
  public interface BatchDataColumnWriter {
    void write(BatchData batchData, long time, Column column, int index);
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

  @FunctionalInterface
  public interface ChunkMetadataStatisticsConverter {
    Statistics<?> convert(
        IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics);
  }

  @FunctionalInterface
  public interface OpcDaTabletValueSetter {
    WTypes.BSTR set(Variant.VARIANT value, Object column, int rowIndex);
  }
}
