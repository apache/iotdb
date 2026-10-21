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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.function.TableFunctionProcessorMemoryBudget;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

/**
 * Largest-Triangle-Three-Buckets (LTTB) downsampling as a built-in Table Model table function.
 *
 * <p>The function shares its table-argument conventions with {@link M4TableFunction}: {@code DATA}
 * has set semantics, {@code TIMECOL} names the {@code TIMESTAMP} ordering column, partition columns
 * are echoed into the proper output, and every other numeric column is a participant column that is
 * downsampled independently. Two mutually exclusive execution modes exist:
 *
 * <ul>
 *   <li><b>Target-count mode</b> ({@code N}): the whole partition is buffered and the standard LTTB
 *       algorithm reduces each participant column to exactly {@code N} points (or all points when
 *       the column has no more than {@code N} eligible points). The output uses a fixed {@code
 *       window_index = 0}.
 *   <li><b>Window/bucket mode</b> ({@code SIZE}, optional {@code SLIDE}/{@code ORIGIN}): windows
 *       are built exactly like M4 (time windows for duration literals, count windows for integers),
 *       but {@code SIZE} defines a bucket rather than the input range of a separate standard LTTB
 *       invocation. The result is standard LTTB whose buckets are the windows:
 *       <ul>
 *         <li>Per partition and participant column, the first eligible point {@code F} and the last
 *             eligible point {@code L} that fall in a window are buckets of their own and are
 *             always returned, as in the original algorithm. {@code F} is reported by the first
 *             window that contains it and {@code L} by the last one.
 *         <li>Every other window contributes one bucket made of its remaining eligible points. The
 *             anchor chain starts at {@code F}: for a window bucket {@code A} is the previously
 *             selected point, {@code C} is the average point of the next window bucket that has
 *             points ({@code L} when there is none), ties go to the earliest point, and buckets
 *             without points are skipped. Overlapping windows ({@code SLIDE < SIZE}) keep one
 *             chain, and the state is independent between partitions and columns.
 *       </ul>
 * </ul>
 *
 * <p>Bucket mode emits, per window, up to three rows per participant column: {@code F}, the point
 * selected from the window bucket, and {@code L}. Rows are padded by position as in M4, and a
 * window in which no participant column has an eligible point emits no row. Target-count mode emits
 * as many rows as the column with the most selected points and no row when that number is zero.
 *
 * <p>{@code NULL} participant values are ignored per column; every non-null value, including {@code
 * NaN} and the infinities, is eligible and is reproduced in the output. A non-finite value cannot
 * serve as a triangle vertex, though: the anchor is the most recent finite selected point (or the
 * first finite eligible point), the lookahead average uses finite points only and skips buckets
 * without one, and a bucket without a finite candidate returns its earliest point without moving
 * the anchor. Ties in the triangle area are broken deterministically by the earliest timestamp
 * (stable input order).
 *
 * <p>The timestamps of the input rows of one partition must be unique. As soon as two rows of the
 * same partition carry the same timestamp the query fails with a semantic error ({@code 701}); the
 * check runs at the partition input boundary, before per-column {@code NULL} filtering and before
 * window expansion, so a row that several overlapping windows share is still a single input row.
 * Equal timestamps in different partitions remain valid. Since the input is ordered by {@code
 * TIMECOL}, equal timestamps are adjacent, and the check compares each row with the previous one
 * and carries the last seen timestamp across input blocks.
 */
public class LTTBTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  public static final String N_PARAMETER_NAME = "N";
  public static final String SIZE_PARAMETER_NAME = "SIZE";
  public static final String SLIDE_PARAMETER_NAME = "SLIDE";
  public static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  /** Internal boolean argument injected by the analyzer: {@code true} if SIZE is a duration. */
  public static final String WINDOW_MODE_PARAMETER_NAME = "__LTTB_WINDOW_MODE";

  public static final long UNSPECIFIED_N = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SIZE = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SLIDE = Long.MIN_VALUE;
  public static final long MIN_TARGET_COUNT = 3L;

  /** Hard cap on eligible points buffered per participant column in one partition or window. */
  public static final int MAX_BUFFERED_POINTS = 1 << 22;

  /**
   * Fallback for {@code lttb_buffer_size_in_bytes} when it is not positive. The configured default
   * in {@code iotdb-system.properties.template} is {@code 0}, which {@code IoTDBDescriptor}
   * resolves to {@code min(32 MiB, operator memory / query_thread_count / 2)}; this constant
   * therefore only matches {@link org.apache.iotdb.commons.conf.CommonConfig}'s initial value and
   * keeps a misconfigured node bounded.
   */
  public static final long DEFAULT_MAX_BUFFER_BYTES = 32L * 1024 * 1024;

  public static final String MODE_PROPERTY = "__LTTB_MODE";
  public static final String PARTITION_TYPES_PROPERTY = "__LTTB_PARTITION_TYPES";
  public static final String PARTICIPANT_TYPES_PROPERTY = "__LTTB_PARTICIPANT_TYPES";

  private static final String OUTPUT_WINDOW_START_COLUMN = "window_start";
  private static final String OUTPUT_WINDOW_END_COLUMN = "window_end";
  private static final String OUTPUT_WINDOW_INDEX_COLUMN = "window_index";
  private static final String TIME_SUFFIX = "_time";
  private static final int INITIAL_CAPACITY = 64;
  private static final int NOT_SELECTED = -1;

  /** Execution mode persisted in the {@link MapTableFunctionHandle}. */
  public enum Mode {
    TARGET_COUNT,
    TIME_WINDOW,
    COUNT_WINDOW
  }

  /**
   * The live value of the hot-reloadable {@code lttb_buffer_size_in_bytes} configuration parameter.
   * Values that are not positive fall back to {@link #DEFAULT_MAX_BUFFER_BYTES} so a misconfigured
   * node still bounds memory instead of buffering without limit.
   *
   * <p>This is read once per {@linkplain TableFunctionProcessorProvider}, in {@link
   * #getProcessorProvider(TableFunctionHandle)}. Every processor of that provider enforces the same
   * value, and the operator reserves the same value, so a hot reload cannot make the two differ for
   * a running query; it applies to providers created afterwards.
   */
  public static long maxBufferBytes() {
    long configured = CommonDescriptor.getInstance().getConfig().getLttbBufferSize();
    return configured > 0 ? configured : DEFAULT_MAX_BUFFER_BYTES;
  }

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(N_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_N)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SIZE_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SIZE)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SLIDE_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SLIDE)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    int timeColumnIndex =
        WindowTVFUtils.checkOrderByColumn(arguments, DATA_PARAMETER_NAME, TIMECOL_PARAMETER_NAME);
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    long n = getLong(arguments, N_PARAMETER_NAME);
    long size = getLong(arguments, SIZE_PARAMETER_NAME);
    long slide = getLong(arguments, SLIDE_PARAMETER_NAME);
    boolean nSpecified = n != UNSPECIFIED_N;
    boolean sizeSpecified = size != UNSPECIFIED_SIZE;
    boolean slideSpecified = slide != UNSPECIFIED_SLIDE;
    if (nSpecified == sizeSpecified) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_EXACTLY_ONE_OF_THE_N_AND_SIZE_ARGUMENTS_MUST_BE_SPECIFIED_FOR_LTTB_54AF0733);
    }

    Mode mode;
    if (nSpecified) {
      if (slideSpecified) {
        throw new SemanticException(
            CommonMessages
                .EXCEPTION_THE_N_ARGUMENT_OF_LTTB_CANNOT_BE_COMBINED_WITH_THE_SLIDE_OR_ORIGIN_ARGUMENTS_C2FF1FE1);
      }
      if (n < MIN_TARGET_COUNT) {
        throw new SemanticException(
            CommonMessages.EXCEPTION_THE_N_ARGUMENT_OF_LTTB_MUST_BE_AT_LEAST_3_29CE2B87);
      }
      mode = Mode.TARGET_COUNT;
    } else {
      if (size <= 0) {
        throw new UDFException(
            CommonMessages
                .EXCEPTION_INVALID_SCALAR_ARGUMENT_SIZE_SHOULD_BE_A_POSITIVE_VALUE_3ECF76E0);
      }
      if (!slideSpecified) {
        slide = size;
      } else if (slide <= 0) {
        throw new UDFException(
            CommonMessages
                .EXCEPTION_INVALID_SCALAR_ARGUMENT_SLIDE_SHOULD_BE_A_POSITIVE_VALUE_F019E091);
      }
      boolean isTimeWindow =
          arguments.containsKey(WINDOW_MODE_PARAMETER_NAME)
              && (boolean) ((ScalarArgument) arguments.get(WINDOW_MODE_PARAMETER_NAME)).getValue();
      mode = isTimeWindow ? Mode.TIME_WINDOW : Mode.COUNT_WINDOW;
    }

    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();
    if (mode == Mode.TIME_WINDOW) {
      schemaBuilder
          .addField(OUTPUT_WINDOW_START_COLUMN, Type.TIMESTAMP)
          .addField(OUTPUT_WINDOW_END_COLUMN, Type.TIMESTAMP);
    } else {
      schemaBuilder.addField(OUTPUT_WINDOW_INDEX_COLUMN, Type.INT64);
    }

    List<Type> partitionTypes = new ArrayList<>();
    for (int partitionIndex : partitionIndexes) {
      Type type = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(type);
      schemaBuilder.addField(tableArgument.getFieldNames().get(partitionIndex).get(), type);
    }

    // Rejects every non-numeric participant column with the shared "not allowed columns" message.
    List<Integer> participantIndexes =
        WindowTVFUtils.getCalculationIndexes(tableArgument, excludedIndexes, null);
    if (participantIndexes.isEmpty()) {
      throw new SemanticException(CommonMessages.EXCEPTION_NO_CALCULATE_COLUMNS);
    }
    List<Type> participantTypes = new ArrayList<>();
    for (int participantIndex : participantIndexes) {
      Type type = tableArgument.getFieldTypes().get(participantIndex);
      String columnName = tableArgument.getFieldNames().get(participantIndex).get();
      participantTypes.add(type);
      schemaBuilder.addField(columnName + TIME_SUFFIX, Type.TIMESTAMP);
      schemaBuilder.addField(columnName, type);
    }

    MapTableFunctionHandle.Builder handleBuilder =
        new MapTableFunctionHandle.Builder()
            .addProperty(MODE_PROPERTY, mode.name())
            .addProperty(PARTITION_TYPES_PROPERTY, WindowTVFUtils.joinTypes(partitionTypes))
            .addProperty(PARTICIPANT_TYPES_PROPERTY, WindowTVFUtils.joinTypes(participantTypes));
    if (mode == Mode.TARGET_COUNT) {
      handleBuilder.addProperty(N_PARAMETER_NAME, n);
    } else {
      handleBuilder.addProperty(SIZE_PARAMETER_NAME, size).addProperty(SLIDE_PARAMETER_NAME, slide);
      if (mode == Mode.TIME_WINDOW) {
        handleBuilder.addProperty(ORIGIN_PARAMETER_NAME, getLong(arguments, ORIGIN_PARAMETER_NAME));
      }
    }

    List<Integer> requiredColumns = new ArrayList<>();
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(partitionIndexes);
    requiredColumns.addAll(participantIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
        .handle(handleBuilder.build())
        .build();
  }

  private static long getLong(Map<String, Argument> arguments, String name) {
    return (long) ((ScalarArgument) arguments.get(name)).getValue();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    Mode mode = Mode.valueOf((String) handle.getProperty(MODE_PROPERTY));
    Type[] partitionTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));
    Type[] participantTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTICIPANT_TYPES_PROPERTY));
    // Resolved once so that every processor of this provider enforces the same limit the operator
    // reserves, even if lttb_buffer_size_in_bytes is hot-reloaded while the query runs.
    long maxBufferedBytes = maxBufferBytes();

    return new LTTBProcessorProvider(
        mode, partitionTypes, participantTypes, handle, maxBufferedBytes);
  }

  /**
   * The provider of one LTTB invocation. It reports the budget it resolved at construction, so the
   * operator reserves exactly the limit its processors enforce.
   */
  private static final class LTTBProcessorProvider
      implements TableFunctionProcessorProvider, TableFunctionProcessorMemoryBudget {
    private final Mode mode;
    private final Type[] partitionTypes;
    private final Type[] participantTypes;
    private final MapTableFunctionHandle handle;
    private final long maxBufferedBytes;

    private LTTBProcessorProvider(
        Mode mode,
        Type[] partitionTypes,
        Type[] participantTypes,
        MapTableFunctionHandle handle,
        long maxBufferedBytes) {
      this.mode = mode;
      this.partitionTypes = partitionTypes;
      this.participantTypes = participantTypes;
      this.handle = handle;
      this.maxBufferedBytes = maxBufferedBytes;
    }

    @Override
    public long getEstimatedMaxBufferedBytes() {
      return maxBufferedBytes;
    }

    @Override
    public TableFunctionDataProcessor getDataProcessor() {
      ParticipantColumn[] participantColumns =
          createParticipantColumns(participantTypes, partitionTypes.length + 1);
      switch (mode) {
        case TARGET_COUNT:
          return new TargetCountLTTBDataProcessor(
              (long) handle.getProperty(N_PARAMETER_NAME),
              partitionTypes,
              participantColumns,
              maxBufferedBytes);
        case TIME_WINDOW:
          return new TimeWindowLTTBDataProcessor(
              (long) handle.getProperty(SIZE_PARAMETER_NAME),
              (long) handle.getProperty(SLIDE_PARAMETER_NAME),
              (long) handle.getProperty(ORIGIN_PARAMETER_NAME),
              partitionTypes,
              participantColumns,
              maxBufferedBytes);
        case COUNT_WINDOW:
          return new CountWindowLTTBDataProcessor(
              (long) handle.getProperty(SIZE_PARAMETER_NAME),
              (long) handle.getProperty(SLIDE_PARAMETER_NAME),
              partitionTypes,
              participantColumns,
              maxBufferedBytes);
        default:
          throw new IllegalStateException(String.valueOf(mode));
      }
    }
  }

  private static ParticipantColumn[] createParticipantColumns(Type[] types, int firstInputIndex) {
    ParticipantColumn[] columns = new ParticipantColumn[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] = new ParticipantColumn(firstInputIndex + i, types[i]);
    }
    return columns;
  }

  // ---------------------------------------------------------------------------------------------
  // LTTB core
  // ---------------------------------------------------------------------------------------------

  /**
   * Standard LTTB selection over an ordered point sequence.
   *
   * <p>When {@code count <= n} every index is returned. Otherwise the first and last points are
   * retained, bucket {@code i} covers {@code [floor(i*(count-2)/(n-2))+1,
   * floor((i+1)*(count-2)/(n-2))+1)}, and for every bucket the candidate forming the largest
   * triangle with the anchor and the lookahead average wins. The last bucket uses the final point
   * as the lookahead, and ties are broken by the earliest timestamp.
   *
   * <p>{@code NaN} and infinite values are eligible points, but they cannot serve as a triangle
   * vertex: an undefined area would make every candidate tie and the bucket would return its
   * earliest point regardless of the data. The anchor is therefore the most recent finite selected
   * point (or the first finite point), the lookahead average uses only finite points and skips
   * buckets without one, and a bucket with no finite candidate returns its earliest point without
   * moving the anchor. Non-finite points are still selected and returned when their bucket has no
   * finite alternative.
   *
   * @return ascending indexes of the selected points
   */
  static int[] selectTargetCount(long[] times, double[] values, int count, long n) {
    return selectTargetCount(times, values, null, count, n);
  }

  private static int[] selectTargetCount(
      long[] times, double[] values, long[] integralValues, int count, long n) {
    if (count <= n) {
      int[] all = new int[count];
      for (int i = 0; i < count; i++) {
        all[i] = i;
      }
      return all;
    }

    int[] selected = new int[(int) n];
    int selectedCount = 0;
    selected[selectedCount++] = 0;
    long bucketCount = n - 2;
    long intermediate = count - 2L;
    // The anchor starts at the earliest finite point so that every area it participates in has a
    // representable origin. It only moves to a selected point; a bucket with no finite candidate
    // leaves it in place.
    int anchor = 0;
    while (anchor < count && !isFiniteValue(values, integralValues, anchor)) {
      anchor++;
    }
    boolean hasAnchor = anchor < count;
    for (long bucket = 0; bucket < bucketCount; bucket++) {
      int currentStart = (int) (bucket * intermediate / bucketCount) + 1;
      int currentEnd = (int) ((bucket + 1) * intermediate / bucketCount) + 1;
      int nextEnd =
          bucket == bucketCount - 1 ? count : (int) ((bucket + 2) * intermediate / bucketCount) + 1;

      long anchorTime = times[hasAnchor ? anchor : 0];
      double anchorValue = hasAnchor ? values[anchor] : 0;
      long integralAnchor = hasAnchor && integralValues != null ? integralValues[anchor] : 0;
      double avgTime;
      double avgValue;
      int lookaheadEnd;
      if (bucket == bucketCount - 1) {
        lookaheadEnd = count;
      } else {
        lookaheadEnd = nextEnd;
      }
      if (bucket == bucketCount - 1) {
        avgTime = differenceAsDouble(times[count - 1], anchorTime);
        avgValue = relativeValue(values, integralValues, count - 1, anchorValue, integralAnchor);
      } else {
        double sumTime = 0;
        double sumValue = 0;
        int finiteCount = 0;
        for (int i = currentEnd; i < lookaheadEnd; i++) {
          if (!isFiniteValue(values, integralValues, i)) {
            continue;
          }
          sumTime += differenceAsDouble(times[i], anchorTime);
          sumValue += relativeValue(values, integralValues, i, anchorValue, integralAnchor);
          finiteCount++;
        }
        if (finiteCount == 0) {
          // No finite point in the next bucket: use the last finite point of the partition.
          int fallback = count - 1;
          while (fallback >= 0 && !isFiniteValue(values, integralValues, fallback)) {
            fallback--;
          }
          if (fallback < 0) {
            fallback = count - 1;
          }
          avgTime = differenceAsDouble(times[fallback], anchorTime);
          avgValue = relativeValue(values, integralValues, fallback, anchorValue, integralAnchor);
        } else {
          avgTime = sumTime / finiteCount;
          avgValue = sumValue / finiteCount;
        }
      }

      int pick =
          argMaxArea(
              times,
              values,
              integralValues,
              currentStart,
              currentEnd,
              anchorTime,
              anchorValue,
              integralAnchor,
              avgTime,
              avgValue);
      if (pick == NOT_SELECTED) {
        pick =
            argMaxAreaExactly(
                times,
                values,
                integralValues,
                currentStart,
                currentEnd,
                anchorTime,
                anchorValue,
                integralAnchor,
                times,
                values,
                integralValues,
                currentEnd,
                lookaheadEnd);
      }
      selected[selectedCount++] = pick;
      if (isFiniteValue(values, integralValues, pick)) {
        anchor = pick;
        hasAnchor = true;
      }
    }
    selected[selectedCount] = count - 1;
    return selected;
  }

  /**
   * Area of the triangle formed by the candidate {@code index}, the anchor and the lookahead point,
   * or {@code -1} when no finite area exists. Areas are non-negative, so {@code -1} unambiguously
   * marks a candidate that cannot be scored.
   *
   * <p>A {@code NaN} or infinite participant value yields a non-finite (undefined) area rather than
   * an overflow: no arithmetic can make it finite. Such a candidate never beats a finite one, so a
   * bucket whose candidates are all non-finite keeps its earliest point.
   */
  private static double boundedArea(
      long[] times,
      double[] values,
      long[] integralValues,
      int index,
      long anchorTime,
      double anchorValue,
      long integralAnchor,
      double nextTime,
      double nextValue) {
    double area =
        Math.abs(
            nextTime * relativeValue(values, integralValues, index, anchorValue, integralAnchor)
                - differenceAsDouble(times[index], anchorTime) * nextValue);
    return Double.isFinite(area) ? area : -1;
  }

  /** Whether the participant value at {@code index} is representable as an exact decimal. */
  private static boolean hasExactValue(double[] values, long[] integralValues, int index) {
    return integralValues != null || Double.isFinite(values[index]);
  }

  /** Whether the participant value at {@code index} can serve as a triangle vertex. */
  private static boolean isFiniteValue(double[] values, long[] integralValues, int index) {
    return integralValues != null || Double.isFinite(values[index]);
  }

  private static int argMaxArea(
      long[] times,
      double[] values,
      long[] integralValues,
      int start,
      int end,
      long anchorTime,
      double anchorValue,
      long integralAnchor,
      double nextTime,
      double nextValue) {
    // An undefined area means one of the vertices is not representable exactly (NaN or infinity);
    // such a candidate cannot be scored. An overflow over representable input is different: the
    // exact fallback recomputes it from the original points, so the whole bucket switches to it —
    // but only when the anchor is itself representable, because the fallback converts it directly.
    boolean exactIsPossible = Double.isFinite(anchorValue);
    int pick = start;
    double maxArea = -1;
    for (int i = start; i < end; i++) {
      double area =
          boundedArea(
              times,
              values,
              integralValues,
              i,
              anchorTime,
              anchorValue,
              integralAnchor,
              nextTime,
              nextValue);
      if (area < 0) {
        if (!hasExactValue(values, integralValues, i)) {
          // Non-finite participant: boundedArea scored it as the smallest area.
          continue;
        }
        if (exactIsPossible) {
          // Overflow on representable input: let the caller compare the whole bucket exactly.
          return NOT_SELECTED;
        }
      }
      if (area > maxArea) {
        maxArea = area;
        pick = i;
      }
    }
    return pick;
  }

  /**
   * Overflow fallback for input values representable as exact decimals. Recomputes every candidate
   * so an overflowing area cannot tie with or hide a larger one. Multiplying all areas by the
   * lookahead point count lets us use sums instead of dividing by that count, keeping the
   * comparison exact even for recurring averages. The normal path does not allocate decimal values.
   *
   * <p>A {@code NaN} or infinite participant cannot be represented by {@link BigDecimal}, so such a
   * candidate is skipped and ties resolve to the earliest point. The caller only reaches this
   * method when the anchor and the lookahead point are finite, so the {@link BigDecimal}
   * conversions of those two origins and of the lookahead values are always well defined.
   */
  private static int argMaxAreaExactly(
      long[] times,
      double[] values,
      long[] integralValues,
      int start,
      int end,
      long anchorTime,
      double anchorValue,
      long integralAnchor,
      long[] nextTimes,
      double[] nextValues,
      long[] nextIntegralValues,
      int nextStart,
      int nextEnd) {
    BigDecimal timeOrigin = BigDecimal.valueOf(anchorTime);
    BigDecimal valueOrigin =
        integralValues == null ? new BigDecimal(anchorValue) : BigDecimal.valueOf(integralAnchor);
    BigDecimal sumTime = BigDecimal.ZERO;
    BigDecimal sumValue = BigDecimal.ZERO;
    for (int i = nextStart; i < nextEnd; i++) {
      if (!hasExactValue(nextValues, nextIntegralValues, i)) {
        continue;
      }
      sumTime = sumTime.add(BigDecimal.valueOf(nextTimes[i]).subtract(timeOrigin));
      sumValue = sumValue.add(exactValue(nextValues, nextIntegralValues, i).subtract(valueOrigin));
    }
    int pick = start;
    BigDecimal maxArea = null;
    for (int i = start; i < end; i++) {
      if (!hasExactValue(values, integralValues, i)) {
        continue;
      }
      BigDecimal time = BigDecimal.valueOf(times[i]).subtract(timeOrigin);
      BigDecimal value = exactValue(values, integralValues, i).subtract(valueOrigin);
      BigDecimal area = sumTime.multiply(value).subtract(time.multiply(sumValue)).abs();
      if (maxArea == null || area.compareTo(maxArea) > 0) {
        maxArea = area;
        pick = i;
      }
    }
    return pick;
  }

  private static BigDecimal exactValue(double[] values, long[] integralValues, int index) {
    // The constructor preserves the actual binary floating-point value, including tiny deltas.
    return integralValues == null
        ? new BigDecimal(values[index])
        : BigDecimal.valueOf(integralValues[index]);
  }

  private static double relativeValue(
      double[] values, long[] integralValues, int index, double origin, long integralOrigin) {
    return integralValues == null
        ? values[index] - origin
        : differenceAsDouble(integralValues[index], integralOrigin);
  }

  private static double differenceAsDouble(long value, long origin) {
    long difference = value - origin;
    // Subtract before converting so nearby integers retain their precision. For spans exceeding
    // the long range, use floating-point subtraction instead of the overflowed long difference.
    return ((value ^ origin) & (value ^ difference)) < 0
        ? (double) value - (double) origin
        : (double) difference;
  }

  // ---------------------------------------------------------------------------------------------
  // Column access and buffering
  // ---------------------------------------------------------------------------------------------

  /** Reservations include array capacity, including both old and new arrays during growth. */
  static final class BufferMemoryBudget {
    private final long limit;
    private long reservedBytes;

    BufferMemoryBudget(long limit) {
      this.limit = limit;
    }

    void reserve(long bytes) {
      if (bytes > limit - reservedBytes) {
        throw new SemanticException(
            String.format(
                CommonMessages
                    .EXCEPTION_LTTB_BUFFERS_EXCEED_LTTB_BUFFER_SIZE_IN_BYTES_ARG_BYTES_26BDE62D,
                limit));
      }
      reservedBytes += bytes;
    }

    void release(long bytes) {
      reservedBytes -= bytes;
    }
  }

  private static final class ParticipantColumn {
    private final int inputIndex;
    private final Type type;

    private ParticipantColumn(int inputIndex, Type type) {
      this.inputIndex = inputIndex;
      this.type = type;
    }

    private boolean isIntegral() {
      return type == Type.INT32 || type == Type.INT64;
    }

    /**
     * Appends the record's value to {@code buffer} if it is non-null. All non-null values are
     * eligible, including {@code NaN} and the infinities: the definition only excludes {@code NULL}
     * participants, and dropping non-finite values would silently turn populated input into an
     * all-{@code NULL} result.
     */
    private void collect(Record record, long time, PointBuffer buffer) {
      if (record.isNull(inputIndex)) {
        return;
      }
      switch (type) {
        case INT32:
          buffer.addIntegral(time, record.getInt(inputIndex));
          break;
        case INT64:
          buffer.addIntegral(time, record.getLong(inputIndex));
          break;
        case FLOAT:
          buffer.addFloating(time, record.getFloat(inputIndex));
          break;
        case DOUBLE:
          buffer.addFloating(time, record.getDouble(inputIndex));
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  CommonMessages.EXCEPTION_UNSUPPORTED_LTTB_VALUE_TYPE_ARG_3E60F6FE, type));
      }
    }

    private boolean isNull(Record record) {
      return record.isNull(inputIndex);
    }

    /** The value as a double, which is lossy only for INT64 values above 2^53. */
    private double floatingValue(Record record) {
      switch (type) {
        case INT32:
          return record.getInt(inputIndex);
        case INT64:
          return record.getLong(inputIndex);
        case FLOAT:
          return record.getFloat(inputIndex);
        case DOUBLE:
          return record.getDouble(inputIndex);
        default:
          throw new IllegalArgumentException(
              String.format(
                  CommonMessages.EXCEPTION_UNSUPPORTED_LTTB_VALUE_TYPE_ARG_3E60F6FE, type));
      }
    }

    /** The exact value of an integral column, or {@code 0} for a floating-point column. */
    private long integralValue(Record record) {
      switch (type) {
        case INT32:
          return record.getInt(inputIndex);
        case INT64:
          return record.getLong(inputIndex);
        default:
          return 0L;
      }
    }

    private void write(
        ColumnBuilder timeBuilder, ColumnBuilder valueBuilder, PointBuffer buffer, int index) {
      writePoint(
          timeBuilder,
          valueBuilder,
          buffer.times[index],
          buffer.values[index],
          buffer.integralValues == null ? 0L : buffer.integralValues[index]);
    }

    /** Writes one point; the integral value is only meaningful for an integral column. */
    private void writePoint(
        ColumnBuilder timeBuilder,
        ColumnBuilder valueBuilder,
        long time,
        double value,
        long integral) {
      timeBuilder.writeLong(time);
      switch (type) {
        case INT32:
          valueBuilder.writeInt((int) integral);
          break;
        case INT64:
          valueBuilder.writeLong(integral);
          break;
        case FLOAT:
          valueBuilder.writeFloat((float) value);
          break;
        case DOUBLE:
          valueBuilder.writeDouble(value);
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  CommonMessages.EXCEPTION_UNSUPPORTED_LTTB_VALUE_TYPE_ARG_3E60F6FE, type));
      }
    }
  }

  /**
   * Growable buffer of eligible points for one participant column. Integral columns additionally
   * keep the exact {@code long} value so INT64 output is not rounded through {@code double}.
   */
  static final class PointBuffer {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(PointBuffer.class);

    private final BufferMemoryBudget memoryBudget;
    private long[] times;
    private double[] values;
    private long[] integralValues;
    private int size;

    PointBuffer(boolean integral, BufferMemoryBudget memoryBudget) {
      this.memoryBudget = memoryBudget;
      if (memoryBudget != null) {
        memoryBudget.reserve(INSTANCE_SIZE + arrayBytes(INITIAL_CAPACITY, integral));
      }
      times = new long[INITIAL_CAPACITY];
      values = new double[INITIAL_CAPACITY];
      if (integral) {
        integralValues = new long[INITIAL_CAPACITY];
      }
    }

    private static long arrayBytes(int capacity, boolean integral) {
      return RamUsageEstimator.sizeOfLongArray(capacity)
          + RamUsageEstimator.sizeOfDoubleArray(capacity)
          + (integral ? RamUsageEstimator.sizeOfLongArray(capacity) : 0);
    }

    private void release() {
      if (memoryBudget != null) {
        memoryBudget.release(INSTANCE_SIZE + arrayBytes(times.length, integralValues != null));
      }
    }

    void addFloating(long time, double value) {
      ensureCapacity();
      times[size] = time;
      values[size] = value;
      size++;
    }

    void addIntegral(long time, long value) {
      ensureCapacity();
      times[size] = time;
      values[size] = value;
      integralValues[size] = value;
      size++;
    }

    /** Appends a point whose exact integral value is only meaningful for an integral column. */
    void add(long time, double value, long integral) {
      if (integralValues == null) {
        addFloating(time, value);
      } else {
        ensureCapacity();
        times[size] = time;
        values[size] = value;
        integralValues[size] = integral;
        size++;
      }
    }

    /** Drops the last point, which is used to move the partition's last eligible point out. */
    void truncateLast() {
      size--;
    }

    /** Average timestamp of the finite points, relative to {@code origin}. */
    double finiteAverageTime(long origin) {
      double sum = 0;
      int count = 0;
      for (int i = 0; i < size; i++) {
        if (!Double.isFinite(values[i])) {
          continue;
        }
        sum += differenceAsDouble(times[i], origin);
        count++;
      }
      return sum / count;
    }

    /** Average value of the finite points, relative to {@code origin}. */
    double finiteAverageValue(double origin, long integralOrigin) {
      double sum = 0;
      int count = 0;
      for (int i = 0; i < size; i++) {
        if (!Double.isFinite(values[i])) {
          continue;
        }
        sum += relativeValue(values, integralValues, i, origin, integralOrigin);
        count++;
      }
      return sum / count;
    }

    /** Whether at least one point has a finite value. */
    boolean hasFinitePoint() {
      for (int i = 0; i < size; i++) {
        if (Double.isFinite(values[i])) {
          return true;
        }
      }
      return false;
    }

    private void ensureCapacity() {
      if (size < times.length) {
        return;
      }
      if (size >= MAX_BUFFERED_POINTS) {
        throw new SemanticException(
            String.format(
                CommonMessages
                    .EXCEPTION_LTTB_BUFFERS_AT_MOST_ARG_ELIGIBLE_POINTS_PER_COLUMN_IN_ONE_PARTITION_OR_WINDOW_4C746D82,
                MAX_BUFFERED_POINTS));
      }
      int newCapacity = (int) Math.min((long) times.length * 2, MAX_BUFFERED_POINTS);
      long oldArrayBytes = arrayBytes(times.length, integralValues != null);
      if (memoryBudget != null) {
        // Reserve before copying: the old arrays remain live until their replacements exist.
        memoryBudget.reserve(arrayBytes(newCapacity, integralValues != null));
      }
      times = Arrays.copyOf(times, newCapacity);
      values = Arrays.copyOf(values, newCapacity);
      if (integralValues != null) {
        integralValues = Arrays.copyOf(integralValues, newCapacity);
      }
      if (memoryBudget != null) {
        memoryBudget.release(oldArrayBytes);
      }
    }

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    long time(int index) {
      return times[index];
    }

    double value(int index) {
      return values[index];
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Processors
  // ---------------------------------------------------------------------------------------------

  /**
   * Partition-local timestamp uniqueness of the input rows. The input is ordered by {@code
   * TIMECOL}, so a duplicate timestamp is always adjacent to its predecessor; the last accepted
   * timestamp is kept in the instance so the rule also spans input blocks of the same partition.
   */
  static final class TimestampUniquenessChecker {
    private long previousTimestamp;
    private boolean hasPreviousTimestamp;

    void check(long time) {
      if (hasPreviousTimestamp && time == previousTimestamp) {
        throw new SemanticException(
            String.format(
                CommonMessages
                    .EXCEPTION_LTTB_DOES_NOT_SUPPORT_DUPLICATE_TIMESTAMPS_BUT_TIMESTAMP_ARG_APPEARS_MORE_THAN_ONCE_IN_THE_SAME_PARTITION_7713FEDD,
                time));
      }
      previousTimestamp = time;
      hasPreviousTimestamp = true;
    }
  }

  private abstract static class AbstractLTTBDataProcessor implements TableFunctionDataProcessor {
    protected final Type[] partitionTypes;
    protected final Object[] partitionValues;
    protected final ParticipantColumn[] participantColumns;
    private final TimestampUniquenessChecker timestampChecker = new TimestampUniquenessChecker();

    protected AbstractLTTBDataProcessor(
        Type[] partitionTypes, ParticipantColumn[] participantColumns) {
      this.partitionTypes = partitionTypes;
      this.partitionValues = new Object[partitionTypes.length];
      this.participantColumns = participantColumns;
    }

    /**
     * Rejects a repeated input timestamp of the partition. Runs at the partition input boundary,
     * before per-column {@code NULL} filtering and before window expansion, so a row shared by
     * several overlapping windows is still a single input row.
     */
    protected final void checkTimestamp(long time) {
      timestampChecker.check(time);
    }

    protected final PointBuffer[] newBuffers(BufferMemoryBudget memoryBudget) {
      PointBuffer[] buffers = new PointBuffer[participantColumns.length];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = new PointBuffer(participantColumns[i].isIntegral(), memoryBudget);
      }
      return buffers;
    }

    /** Partition columns are constant within a partition; keep the first non-null value. */
    protected final void capturePartitionValues(Record input) {
      for (int i = 0; i < partitionTypes.length; i++) {
        int inputIndex = i + 1;
        if (partitionValues[i] == null && !input.isNull(inputIndex)) {
          partitionValues[i] = WindowTVFUtils.readValue(input, inputIndex, partitionTypes[i]);
        }
      }
    }

    protected final void collect(Record input, long time, PointBuffer[] buffers) {
      for (int i = 0; i < participantColumns.length; i++) {
        participantColumns[i].collect(input, time, buffers[i]);
      }
    }

    /**
     * Writes one output row. {@code windowColumnCount} leading builders were already filled by the
     * caller; {@code selectedIndexes[i]} is {@link #NOT_SELECTED} when column {@code i} has no
     * point.
     */
    protected final void writeRow(
        List<ColumnBuilder> builders,
        int windowColumnCount,
        PointBuffer[] buffers,
        int[] selectedIndexes) {
      int builderIndex = windowColumnCount;
      for (int i = 0; i < partitionTypes.length; i++) {
        WindowTVFUtils.writeValue(
            builders.get(builderIndex++), partitionValues[i], partitionTypes[i]);
      }
      for (int i = 0; i < participantColumns.length; i++) {
        ColumnBuilder timeBuilder = builders.get(builderIndex++);
        ColumnBuilder valueBuilder = builders.get(builderIndex++);
        if (selectedIndexes[i] == NOT_SELECTED) {
          timeBuilder.appendNull();
          valueBuilder.appendNull();
        } else {
          participantColumns[i].write(timeBuilder, valueBuilder, buffers[i], selectedIndexes[i]);
        }
      }
    }
  }

  /** Buffers the whole partition and applies the standard LTTB algorithm in {@link #finish}. */
  private static final class TargetCountLTTBDataProcessor extends AbstractLTTBDataProcessor {
    private final long n;
    private final PointBuffer[] buffers;
    private final BufferMemoryBudget memoryBudget;
    private boolean hasRows;

    private TargetCountLTTBDataProcessor(
        long n,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns,
        long maxBufferedBytes) {
      super(partitionTypes, participantColumns);
      this.n = n;
      this.memoryBudget = new BufferMemoryBudget(maxBufferedBytes);
      memoryBudget.reserve(RamUsageEstimator.sizeOfObjectArray(participantColumns.length));
      this.buffers = newBuffers(memoryBudget);
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      hasRows = true;
      long time = input.getLong(0);
      checkTimestamp(time);
      capturePartitionValues(input);
      collect(input, time, buffers);
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (!hasRows) {
        return;
      }
      long selectionBytes =
          RamUsageEstimator.sizeOfObjectArray(participantColumns.length)
              + RamUsageEstimator.sizeOfIntArray(participantColumns.length);
      for (PointBuffer buffer : buffers) {
        selectionBytes += RamUsageEstimator.sizeOfIntArray((int) Math.min(buffer.size, n));
      }
      memoryBudget.reserve(selectionBytes);
      int[][] selections = new int[participantColumns.length][];
      int rowCount = 0;
      for (int i = 0; i < participantColumns.length; i++) {
        PointBuffer buffer = buffers[i];
        selections[i] =
            selectTargetCount(buffer.times, buffer.values, buffer.integralValues, buffer.size, n);
        rowCount = Math.max(rowCount, selections[i].length);
      }

      int[] selectedIndexes = new int[participantColumns.length];
      for (int row = 0; row < rowCount; row++) {
        properColumnBuilders.get(0).writeLong(0L);
        for (int i = 0; i < participantColumns.length; i++) {
          selectedIndexes[i] = row < selections[i].length ? selections[i][row] : NOT_SELECTED;
        }
        writeRow(properColumnBuilders, 1, buffers, selectedIndexes);
      }
    }

    @Override
    public void beforeDestroy() {
      Arrays.fill(buffers, null);
      memoryBudget.reservedBytes = 0;
    }
  }

  /**
   * One window of the partition: the eligible points that fall in it, per participant column. The
   * first and last eligible point of a partition are not stored here, because they form buckets of
   * their own.
   */
  private abstract static class WindowState {
    protected final PointBuffer[] buffers;
    protected final long endExclusive;

    private WindowState(PointBuffer[] buffers, long endExclusive) {
      this.buffers = buffers;
      this.endExclusive = endExclusive;
    }

    protected abstract void writeWindowColumns(List<ColumnBuilder> builders);

    protected abstract int getWindowColumnCount();
  }

  private static final class TimeWindowState extends WindowState {
    private final long windowStart;

    private TimeWindowState(PointBuffer[] buffers, long windowStart, long endExclusive) {
      super(buffers, endExclusive);
      this.windowStart = windowStart;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> builders) {
      builders.get(0).writeLong(windowStart);
      builders.get(1).writeLong(endExclusive);
    }

    @Override
    protected int getWindowColumnCount() {
      return 2;
    }
  }

  private static final class CountWindowState extends WindowState {
    private final long windowIndex;

    private CountWindowState(PointBuffer[] buffers, long endExclusive, long windowIndex) {
      super(buffers, endExclusive);
      this.windowIndex = windowIndex;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> builders) {
      builders.get(0).writeLong(windowIndex);
    }

    @Override
    protected int getWindowColumnCount() {
      return 1;
    }
  }

  /** One output cell of window mode: a selected point, or the first/last eligible point. */
  private static final class WindowRow {
    private final long time;
    private final double value;
    private final long integral;

    private WindowRow(long time, double value, long integral) {
      this.time = time;
      this.value = value;
      this.integral = integral;
    }

    private static WindowRow of(PointBuffer buffer, int index) {
      return new WindowRow(
          buffer.times[index],
          buffer.values[index],
          buffer.integralValues == null ? 0L : buffer.integralValues[index]);
    }
  }

  /**
   * Window/bucket mode. Windows are constructed like M4, and {@code SIZE} defines a bucket rather
   * than the input range of a separate standard LTTB invocation; the result is standard LTTB whose
   * buckets are the windows:
   *
   * <ul>
   *   <li>Per partition and participant column, the first eligible point {@code F} and the last
   *       eligible point {@code L} that fall in a window are buckets of their own and are always
   *       returned. {@code F} is reported by the first window that contains it and {@code L} by the
   *       last one, so a window emits up to three rows: {@code F}, its selected point and {@code
   *       L}.
   *   <li>Every other window contributes one bucket made of its remaining eligible points. The
   *       anchor chain starts at {@code F}: for a window bucket {@code A} is the previously
   *       selected point, {@code C} is the average point of the next window bucket that has points
   *       (and {@code L} when there is none), ties go to the earliest point, and buckets without
   *       points are skipped. Overlapping windows ({@code SLIDE < SIZE}) keep one chain.
   *   <li>A window in which no participant column has an eligible point emits no row.
   * </ul>
   *
   * <p>Rows are padded by position, as in M4. {@code L} is only known at the end of the partition,
   * so a window is emitted once the bucket that supplies its {@code C} is closed and known not to
   * end the partition, or in {@link #finish}.
   *
   * <p>Non-finite values stay eligible, but a {@code NaN} or infinite point cannot be a triangle
   * vertex: {@code A} is the most recent finite selected point (or the first finite eligible
   * point), {@code C} averages only finite points and skips buckets without one, falling back to
   * the last finite eligible point, and a bucket without a finite candidate returns its earliest
   * point without moving {@code A}.
   */
  private abstract static class AbstractWindowLTTBDataProcessor<W extends WindowState>
      extends AbstractLTTBDataProcessor {
    private static final long WINDOW_INSTANCE_SIZE =
        Math.max(
            RamUsageEstimator.shallowSizeOfInstance(TimeWindowState.class),
            RamUsageEstimator.shallowSizeOfInstance(CountWindowState.class));

    protected final long size;
    protected final long slide;
    protected final Deque<W> activeWindows = new ArrayDeque<>();
    private final Deque<W> pendingWindows = new ArrayDeque<>();
    private final BufferMemoryBudget memoryBudget;
    private final long windowMetadataBytes;

    private final boolean[] hasFirstEligible;
    private final long[] firstEligibleTimes;
    private final double[] firstEligibleValues;
    private final long[] integralFirstEligibleValues;
    private final WindowState[] firstEligibleWindows;
    private final boolean[] hasLastEligible;
    private final long[] lastEligibleTimes;
    private final double[] lastEligibleValues;
    private final long[] integralLastEligibleValues;
    private final long[] lastEligiblePositions;
    private final boolean[] hasLastFinite;
    private final long[] lastFiniteTimes;
    private final double[] lastFiniteValues;
    private final long[] integralLastFiniteValues;
    private final boolean[] hasFirstFinite;
    private final long[] firstFiniteTimes;
    private final double[] firstFiniteValues;
    private final long[] integralFirstFiniteValues;
    private final boolean[] hasAnchor;
    private final long[] anchorTimes;
    private final double[] anchorValues;
    private final long[] integralAnchorValues;
    private final WindowState[] lastRowWindows;
    private boolean finishing;

    protected AbstractWindowLTTBDataProcessor(
        long size,
        long slide,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns,
        long maxBufferedBytes) {
      super(partitionTypes, participantColumns);
      this.size = size;
      this.slide = slide;
      this.memoryBudget = new BufferMemoryBudget(maxBufferedBytes);
      int columnCount = participantColumns.length;
      this.windowMetadataBytes =
          WINDOW_INSTANCE_SIZE + 3 * RamUsageEstimator.sizeOfObjectArray(columnCount);
      this.hasFirstEligible = new boolean[columnCount];
      this.firstEligibleTimes = new long[columnCount];
      this.firstEligibleValues = new double[columnCount];
      this.integralFirstEligibleValues = new long[columnCount];
      this.firstEligibleWindows = new WindowState[columnCount];
      this.hasLastEligible = new boolean[columnCount];
      this.lastEligibleTimes = new long[columnCount];
      this.lastEligibleValues = new double[columnCount];
      this.integralLastEligibleValues = new long[columnCount];
      this.lastEligiblePositions = new long[columnCount];
      this.hasLastFinite = new boolean[columnCount];
      this.lastFiniteTimes = new long[columnCount];
      this.lastFiniteValues = new double[columnCount];
      this.integralLastFiniteValues = new long[columnCount];
      this.hasFirstFinite = new boolean[columnCount];
      this.firstFiniteTimes = new long[columnCount];
      this.firstFiniteValues = new double[columnCount];
      this.integralFirstFiniteValues = new long[columnCount];
      this.hasAnchor = new boolean[columnCount];
      this.anchorTimes = new long[columnCount];
      this.anchorValues = new double[columnCount];
      this.integralAnchorValues = new long[columnCount];
      this.lastRowWindows = new WindowState[columnCount];
    }

    protected final PointBuffer[] newWindowBuffers() {
      // Even an all-NULL window allocates state, so reserve before creating any of its buffers.
      memoryBudget.reserve(windowMetadataBytes);
      return newBuffers(memoryBudget);
    }

    private void releaseWindow(W window) {
      for (PointBuffer buffer : window.buffers) {
        buffer.release();
      }
      memoryBudget.release(windowMetadataBytes);
    }

    @Override
    public final void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long time = input.getLong(0);
      checkTimestamp(time);
      capturePartitionValues(input);
      processRecord(input, time, properColumnBuilders);
    }

    protected abstract void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders);

    /**
     * Adds one input row to every open window that contains it. The first eligible point of a
     * column is kept aside as the {@code F} bucket instead of being added to a window bucket.
     */
    protected final void collectIntoWindows(long position, Record input, long time) {
      for (int i = 0; i < participantColumns.length; i++) {
        // A point that falls in a gap of a SLIDE > SIZE layout belongs to no window at all.
        if (participantColumns[i].isNull(input) || activeWindows.isEmpty()) {
          continue;
        }
        double value = participantColumns[i].floatingValue(input);
        long integral = participantColumns[i].integralValue(input);
        if (Double.isFinite(value)) {
          if (!hasFirstFinite[i]) {
            hasFirstFinite[i] = true;
            firstFiniteTimes[i] = time;
            firstFiniteValues[i] = value;
            integralFirstFiniteValues[i] = integral;
          }
          hasLastFinite[i] = true;
          lastFiniteTimes[i] = time;
          lastFiniteValues[i] = value;
          integralLastFiniteValues[i] = integral;
        }
        if (!hasFirstEligible[i]) {
          hasFirstEligible[i] = true;
          firstEligibleTimes[i] = time;
          firstEligibleValues[i] = value;
          integralFirstEligibleValues[i] = integral;
          firstEligibleWindows[i] = activeWindows.peekFirst();
          continue;
        }
        hasLastEligible[i] = true;
        lastEligibleTimes[i] = time;
        lastEligibleValues[i] = value;
        integralLastEligibleValues[i] = integral;
        lastEligiblePositions[i] = position;
        for (W window : activeWindows) {
          window.buffers[i].add(time, value, integral);
        }
      }
    }

    /** Moves the windows that ended before {@code position} into the pending queue. */
    protected final void emitClosedWindows(long position, List<ColumnBuilder> builders) {
      while (!activeWindows.isEmpty() && activeWindows.peekFirst().endExclusive <= position) {
        pendingWindows.addLast(activeWindows.removeFirst());
      }
      emitReadyWindows(builders);
    }

    @Override
    public final void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      moveLastEligibleOutOfBuckets();
      emitReadyWindows(properColumnBuilders);
      Arrays.fill(firstEligibleWindows, null);
      Arrays.fill(lastRowWindows, null);
    }

    /**
     * {@code L} is the last eligible point of a column and is a bucket of its own, but it was
     * collected into the window buckets that contain it. Now that the partition has ended, remove
     * it from the bucket it ends and remember which window reports it.
     */
    private void moveLastEligibleOutOfBuckets() {
      finishing = true;
      while (!activeWindows.isEmpty()) {
        pendingWindows.addLast(activeWindows.removeFirst());
      }
      for (WindowState window : pendingWindows) {
        for (int i = 0; i < participantColumns.length; i++) {
          PointBuffer bucket = window.buffers[i];
          if (bucket.isEmpty() || !containsPosition(window, lastEligiblePositions[i])) {
            continue;
          }
          if (bucket.time(bucket.size() - 1) == lastEligibleTimes[i]) {
            bucket.truncateLast();
            lastRowWindows[i] = window;
          }
        }
      }
      for (int i = 0; i < participantColumns.length; i++) {
        if (!hasLastEligible[i]) {
          // A single eligible point is both F and L and was already reported as F.
          lastRowWindows[i] = null;
        }
      }
    }

    @Override
    public final void beforeDestroy() {
      activeWindows.clear();
      pendingWindows.clear();
      Arrays.fill(firstEligibleWindows, null);
      Arrays.fill(lastRowWindows, null);
      // Also covers a reservation made by a window whose construction failed at the limit.
      memoryBudget.reservedBytes = 0;
    }

    protected final void emitReadyWindows(List<ColumnBuilder> builders) {
      while (!pendingWindows.isEmpty() && isReady(pendingWindows.peekFirst())) {
        W window = pendingWindows.removeFirst();
        emitWindow(window, builders);
        releaseWindow(window);
      }
    }

    /**
     * Whether the first pending window can be emitted. A window that contains {@code L} must wait
     * for the end of the partition, and so must a window whose {@code C} would come from a bucket
     * that can still lose {@code L}.
     */
    private boolean isReady(W window) {
      if (finishing) {
        return true;
      }
      for (int i = 0; i < participantColumns.length; i++) {
        if (containsPosition(window, lastEligiblePositions[i])) {
          return false;
        }
        PointBuffer bucket = window.buffers[i];
        if (bucket.isEmpty() || !bucket.hasFinitePoint()) {
          continue;
        }
        WindowState lookahead = findLookahead(window, i);
        if (lookahead == null || containsPosition(lookahead, lastEligiblePositions[i])) {
          return false;
        }
      }
      return true;
    }

    /** The next closed window after {@code window} whose bucket of {@code column} supplies C. */
    private WindowState findLookahead(WindowState window, int column) {
      for (WindowState candidate : pendingWindows) {
        if (candidate.endExclusive <= window.endExclusive) {
          continue;
        }
        PointBuffer bucket = candidate.buffers[column];
        if (!bucket.isEmpty() && bucket.hasFinitePoint()) {
          return candidate;
        }
      }
      return null;
    }

    /**
     * A one-point lookahead used by the exact fallback when {@code C} is the last eligible point.
     */
    private PointBuffer singlePoint(long time, double value, int column) {
      PointBuffer point = new PointBuffer(participantColumns[column].isIntegral(), null);
      point.add(
          time,
          value,
          participantColumns[column].isIntegral() ? integralLastFiniteValues[column] : 0L);
      return point;
    }

    /** Whether {@code position} falls in {@code window}; every window is {@code size} long. */
    private boolean containsPosition(WindowState window, long position) {
      return position >= window.endExclusive - size && position < window.endExclusive;
    }

    private void emitWindow(W window, List<ColumnBuilder> builders) {
      WindowRow[][] rows = new WindowRow[participantColumns.length][];
      int rowCount = 0;
      for (int i = 0; i < participantColumns.length; i++) {
        rows[i] = selectWindowRows(window, i);
        rowCount = Math.max(rowCount, rows[i].length);
      }
      // A window in which no participant column has an eligible point emits nothing.
      for (int row = 0; row < rowCount; row++) {
        window.writeWindowColumns(builders);
        writeWindowRow(builders, window.getWindowColumnCount(), rows, row);
      }
    }

    private void writeWindowRow(
        List<ColumnBuilder> builders, int windowColumnCount, WindowRow[][] rows, int row) {
      int builderIndex = windowColumnCount;
      for (int i = 0; i < partitionTypes.length; i++) {
        WindowTVFUtils.writeValue(
            builders.get(builderIndex++), partitionValues[i], partitionTypes[i]);
      }
      for (int i = 0; i < participantColumns.length; i++) {
        ColumnBuilder timeBuilder = builders.get(builderIndex++);
        ColumnBuilder valueBuilder = builders.get(builderIndex++);
        if (row < rows[i].length) {
          WindowRow cell = rows[i][row];
          participantColumns[i].writePoint(
              timeBuilder, valueBuilder, cell.time, cell.value, cell.integral);
        } else {
          timeBuilder.appendNull();
          valueBuilder.appendNull();
        }
      }
    }

    /**
     * The rows of one window for one participant column, in output order: {@code F}, the point
     * selected from the window bucket, and {@code L}.
     */
    private WindowRow[] selectWindowRows(W window, int column) {
      WindowRow first = window == firstEligibleWindows[column] ? firstEligibleRow(column) : null;
      WindowRow last = window == lastRowWindows[column] ? lastEligibleRow(column) : null;
      PointBuffer bucket = window.buffers[column];
      WindowRow pick = bucket.isEmpty() ? null : pickInBucket(window, column, bucket);
      WindowRow[] result =
          new WindowRow[(first == null ? 0 : 1) + (pick == null ? 0 : 1) + (last == null ? 0 : 1)];
      int index = 0;
      if (first != null) {
        result[index++] = first;
      }
      if (pick != null) {
        result[index++] = pick;
      }
      if (last != null) {
        result[index] = last;
      }
      return result;
    }

    private WindowRow firstEligibleRow(int column) {
      return new WindowRow(
          firstEligibleTimes[column],
          firstEligibleValues[column],
          integralFirstEligibleValues[column]);
    }

    private WindowRow lastEligibleRow(int column) {
      return new WindowRow(
          lastEligibleTimes[column],
          lastEligibleValues[column],
          integralLastEligibleValues[column]);
    }

    /** Selects the point of {@code bucket} with the largest triangle against {@code A} and C. */
    private WindowRow pickInBucket(W window, int column, PointBuffer bucket) {
      long anchorTime;
      double anchorValue;
      long integralAnchor;
      if (hasAnchor[column]) {
        anchorTime = anchorTimes[column];
        anchorValue = anchorValues[column];
        integralAnchor = integralAnchorValues[column];
      } else if (hasFirstFinite[column]) {
        // The chain starts at F, or at the first finite eligible point when F is not finite.
        anchorTime = firstFiniteTimes[column];
        anchorValue = firstFiniteValues[column];
        integralAnchor = integralFirstFiniteValues[column];
      } else {
        // Only non-finite values exist; every area is undefined and the earliest point wins.
        return WindowRow.of(bucket, 0);
      }

      WindowState lookaheadWindow = findLookahead(window, column);
      double nextTime;
      double nextValue;
      PointBuffer exactLookahead;
      if (lookaheadWindow != null) {
        PointBuffer lookahead = lookaheadWindow.buffers[column];
        nextTime = lookahead.finiteAverageTime(anchorTime);
        nextValue = lookahead.finiteAverageValue(anchorValue, integralAnchor);
        exactLookahead = lookahead;
      } else if (finishing && hasLastFinite[column]) {
        // No later bucket carries a finite point: C is the last finite eligible point.
        nextTime = differenceAsDouble(lastFiniteTimes[column], anchorTime);
        nextValue =
            participantColumns[column].isIntegral()
                ? differenceAsDouble(integralLastFiniteValues[column], integralAnchor)
                : lastFiniteValues[column] - anchorValue;
        exactLookahead = singlePoint(lastFiniteTimes[column], lastFiniteValues[column], column);
      } else {
        return WindowRow.of(bucket, 0);
      }

      int pick =
          argMaxArea(
              bucket.times,
              bucket.values,
              bucket.integralValues,
              0,
              bucket.size(),
              anchorTime,
              anchorValue,
              integralAnchor,
              nextTime,
              nextValue);
      if (pick == NOT_SELECTED && exactLookahead != null) {
        pick =
            argMaxAreaExactly(
                bucket.times,
                bucket.values,
                bucket.integralValues,
                0,
                bucket.size(),
                anchorTime,
                anchorValue,
                integralAnchor,
                exactLookahead.times,
                exactLookahead.values,
                exactLookahead.integralValues,
                0,
                exactLookahead.size());
      }
      if (pick == NOT_SELECTED) {
        // The bucket has no finite candidate; return its earliest point and keep A unchanged.
        return WindowRow.of(bucket, 0);
      }
      WindowRow row = WindowRow.of(bucket, pick);
      hasAnchor[column] = true;
      anchorTimes[column] = row.time;
      anchorValues[column] = row.value;
      integralAnchorValues[column] = row.integral;
      return row;
    }
  }

  private static final class TimeWindowLTTBDataProcessor
      extends AbstractWindowLTTBDataProcessor<TimeWindowState> {
    private final long origin;
    private boolean nextWindowStartInitialized = false;
    private boolean windowsExhausted;
    private long nextWindowStart;

    private TimeWindowLTTBDataProcessor(
        long size,
        long slide,
        long origin,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns,
        long maxBufferedBytes) {
      super(size, slide, partitionTypes, participantColumns, maxBufferedBytes);
      this.origin = origin;
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      emitClosedWindows(time, properColumnBuilders);

      if (!windowsExhausted) {
        long firstCandidateStart = getFirstWindowStart(time);
        if (!nextWindowStartInitialized || nextWindowStart < firstCandidateStart) {
          nextWindowStart = firstCandidateStart;
          nextWindowStartInitialized = true;
        }

        while (!windowsExhausted && nextWindowStart <= time) {
          long windowEnd;
          try {
            windowEnd = Math.addExact(nextWindowStart, size);
          } catch (ArithmeticException e) {
            throw windowBoundaryOverflow();
          }
          activeWindows.addLast(
              new TimeWindowState(newWindowBuffers(), nextWindowStart, windowEnd));
          // An unrepresentable next start means there are no more windows, but the current
          // window can still be valid and must collect this and subsequent input rows.
          if (nextWindowStart > Long.MAX_VALUE - slide) {
            windowsExhausted = true;
          } else {
            nextWindowStart += slide;
          }
        }
      }

      collectIntoWindows(time, input, time);
      emitReadyWindows(properColumnBuilders);
    }

    /** First aligned start whose window ends strictly after the current timestamp. */
    private long getFirstWindowStart(long time) {
      try {
        long distance = Math.subtractExact(Math.subtractExact(time, origin), size);
        long steps = Math.addExact(Math.floorDiv(distance, slide), 1);
        return Math.addExact(origin, Math.multiplyExact(steps, slide));
      } catch (ArithmeticException e) {
        // Only intermediate arithmetic may overflow, e.g. time near Long.MIN_VALUE.
        // Fall back to exact arithmetic before deciding whether the actual boundary is valid.
        BigInteger step = BigInteger.valueOf(slide);
        BigInteger[] quotientAndRemainder =
            BigInteger.valueOf(time)
                .subtract(BigInteger.valueOf(origin))
                .subtract(BigInteger.valueOf(size))
                .divideAndRemainder(step);
        BigInteger steps = quotientAndRemainder[0];
        if (quotientAndRemainder[1].signum() >= 0) {
          steps = steps.add(BigInteger.ONE);
        }
        BigInteger start = BigInteger.valueOf(origin).add(steps.multiply(step));
        if (start.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
          // The input lies in a gap and the next aligned window is beyond the time domain.
          windowsExhausted = true;
          return Long.MAX_VALUE;
        }
        try {
          return start.longValueExact();
        } catch (ArithmeticException overflow) {
          throw windowBoundaryOverflow();
        }
      }
    }

    private SemanticException windowBoundaryOverflow() {
      return new SemanticException(
          CommonMessages.EXCEPTION_LTTB_WINDOW_BOUNDARIES_EXCEED_THE_TIMESTAMP_RANGE_7C0FC7E2);
    }
  }

  private static final class CountWindowLTTBDataProcessor
      extends AbstractWindowLTTBDataProcessor<CountWindowState> {
    private long rowCount = 0;
    private long nextWindowStart = 0;
    private long nextWindowIndex = 0;
    private boolean windowsExhausted;

    private CountWindowLTTBDataProcessor(
        long size,
        long slide,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns,
        long maxBufferedBytes) {
      super(size, slide, partitionTypes, participantColumns, maxBufferedBytes);
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      emitClosedWindows(rowCount, properColumnBuilders);

      // Input row positions advance one at a time, so no expired starts need to be skipped.
      // Check the end only when a window is reached: a distant future end may be unrepresentable
      // even though every window that actually receives input is valid.
      if (!windowsExhausted && nextWindowStart == rowCount) {
        long windowEnd;
        try {
          windowEnd = Math.addExact(nextWindowStart, size);
        } catch (ArithmeticException e) {
          throw new SemanticException(
              CommonMessages
                  .EXCEPTION_LTTB_COUNT_WINDOW_BOUNDARIES_EXCEED_THE_INT64_RANGE_5BC980CA);
        }
        activeWindows.addLast(
            new CountWindowState(newWindowBuffers(), windowEnd, nextWindowIndex++));
        if (nextWindowStart > Long.MAX_VALUE - slide) {
          windowsExhausted = true;
        } else {
          nextWindowStart += slide;
        }
      }

      collectIntoWindows(rowCount, input, time);
      rowCount++;
      emitReadyWindows(properColumnBuilders);
    }
  }
}
