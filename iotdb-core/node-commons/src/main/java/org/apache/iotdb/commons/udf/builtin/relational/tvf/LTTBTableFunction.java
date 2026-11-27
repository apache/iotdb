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
import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.function.BatchedTableFunctionDataProcessor;
import org.apache.iotdb.commons.queryengine.plan.relational.function.TableFunctionProcessorMemoryBudget;
import org.apache.iotdb.udf.api.IoTDBLocal;
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
import java.util.function.BooleanSupplier;

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
 * first finite eligible point), {@code C} averages only the finite points of the next bucket that
 * has one — where the final point is the last bucket, and the last finite eligible point stands in
 * when no later bucket has a finite point — and a bucket without a finite candidate returns its
 * earliest point without moving the anchor. Ties in the triangle area are broken deterministically
 * by the earliest timestamp (stable input order).
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

  /**
   * Internal boolean argument injected by the analyzer: {@code true} if the user passed {@code
   * ORIGIN}. The analyzer knows this from the argument list, which is more precise than comparing
   * against {@link #UNSPECIFIED_ORIGIN}: {@link Long#MIN_VALUE} is a legal timestamp.
   */
  public static final String ORIGIN_SPECIFIED_PARAMETER_NAME = "__LTTB_ORIGIN_SPECIFIED";

  public static final long UNSPECIFIED_N = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SIZE = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SLIDE = Long.MIN_VALUE;

  /**
   * Marks {@code ORIGIN} as not specified. The user-visible default of the argument is still {@code
   * 0} (the epoch); the sentinel only lets {@link #analyze} distinguish an explicit {@code ORIGIN}
   * from the injected default, so the function rejects the argument in modes that do not use it.
   */
  public static final long UNSPECIFIED_ORIGIN = Long.MIN_VALUE;

  public static final long MIN_TARGET_COUNT = 3L;

  /** Hard cap on eligible points buffered per participant column in one partition or window. */
  public static final int MAX_BUFFERED_POINTS = 1 << 22;

  /**
   * Fallback for {@code lttb_buffer_size_in_bytes} when it is not positive. The configured default
   * in {@code iotdb-system.properties.template} is {@code 0}, which {@code IoTDBDescriptor}
   * resolves to {@code min(32 MiB, operator memory / query_thread_count / 8)}; this constant
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
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .setSemantics()
            .ascendingOrder()
            .build(),
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
            .defaultValue(UNSPECIFIED_ORIGIN)
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
    long origin = getLong(arguments, ORIGIN_PARAMETER_NAME);
    boolean nSpecified = n != UNSPECIFIED_N;
    boolean sizeSpecified = size != UNSPECIFIED_SIZE;
    boolean slideSpecified = slide != UNSPECIFIED_SLIDE;
    boolean originSpecified =
        arguments.containsKey(ORIGIN_SPECIFIED_PARAMETER_NAME)
            ? (boolean) ((ScalarArgument) arguments.get(ORIGIN_SPECIFIED_PARAMETER_NAME)).getValue()
            : origin != UNSPECIFIED_ORIGIN;
    if (nSpecified == sizeSpecified) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_EXACTLY_ONE_OF_THE_N_AND_SIZE_ARGUMENTS_MUST_BE_SPECIFIED_FOR_LTTB_54AF0733);
    }

    Mode mode;
    if (nSpecified) {
      if (slideSpecified || originSpecified) {
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
      if (!isTimeWindow && originSpecified) {
        throw new SemanticException(
            QueryMessages
                .EXCEPTION_THE_ORIGIN_ARGUMENT_IS_ONLY_SUPPORTED_IN_TIME_WINDOW_MODE_B3F358B9);
      }
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
        handleBuilder.addProperty(ORIGIN_PARAMETER_NAME, originSpecified ? origin : 0L);
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
   * triangle with the anchor and the lookahead average wins. The final point is a bucket of its
   * own, so the last bucket looks ahead to it, and ties are broken by the earliest timestamp.
   *
   * <p>{@code NaN} and infinite values are eligible points, but they cannot serve as a triangle
   * vertex: an undefined area would make every candidate tie and the bucket would return its
   * earliest point regardless of the data. The anchor is therefore the most recent finite selected
   * point (or the first finite point), and {@code C} averages only the finite points of the next
   * bucket that has one — skipping buckets without one, and falling back to the last finite point
   * of the partition when there is no such bucket, which includes a non-finite final point. A
   * bucket with no finite candidate returns its earliest point without moving the anchor.
   * Non-finite points are still selected and returned when their bucket has no finite alternative.
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
    // C averages the finite points of the next bucket that has one, so the lookahead bucket only
    // moves forward and every bucket is scanned at most once. A value past the last bucket marks
    // that there is none left and C becomes the last finite point of the partition.
    int lastFinite = count - 1;
    while (lastFinite >= 0 && !isFiniteValue(values, integralValues, lastFinite)) {
      lastFinite--;
    }
    long lookaheadBucket = -1;
    for (long bucket = 0; bucket < bucketCount; bucket++) {
      int currentStart = bucketBoundary(bucket, intermediate, bucketCount);
      int currentEnd = bucketBoundary(bucket + 1, intermediate, bucketCount);

      long anchorTime = times[hasAnchor ? anchor : 0];
      double anchorValue = hasAnchor ? values[anchor] : 0;
      long integralAnchor = hasAnchor && integralValues != null ? integralValues[anchor] : 0;
      long next = Math.max(bucket + 1, lookaheadBucket);
      while (next <= bucketCount
          && !hasFinitePoint(values, integralValues, next, count, intermediate, bucketCount)) {
        next++;
      }
      lookaheadBucket = next;
      int lookaheadStart;
      int lookaheadEnd;
      if (lookaheadBucket == bucketCount + 1) {
        // No later bucket has a finite point: use the last finite point, or the final point when
        // the whole partition is non-finite.
        lookaheadStart = lastFinite >= 0 ? lastFinite : count - 1;
        lookaheadEnd = lookaheadStart + 1;
      } else {
        lookaheadStart = bucketBoundary(lookaheadBucket, intermediate, bucketCount);
        lookaheadEnd =
            lookaheadBucket == bucketCount
                ? count
                : bucketBoundary(lookaheadBucket + 1, intermediate, bucketCount);
      }
      int pick =
          selectBucketPoint(
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
              lookaheadStart,
              lookaheadEnd);
      selected[selectedCount++] = pick;
      if (isFiniteValue(values, integralValues, pick)) {
        anchor = pick;
        hasAnchor = true;
      }
    }
    selected[selectedCount] = count - 1;
    return selected;
  }

  /** First index of bucket {@code bucket}; bucket {@code bucketCount} is the final point. */
  private static int bucketBoundary(long bucket, long intermediate, long bucketCount) {
    return (int) (bucket * intermediate / bucketCount) + 1;
  }

  /** Whether {@code bucket} holds a point that can serve as {@code C}. */
  private static boolean hasFinitePoint(
      double[] values,
      long[] integralValues,
      long bucket,
      int count,
      long intermediate,
      long bucketCount) {
    int start = bucketBoundary(bucket, intermediate, bucketCount);
    int end = bucket == bucketCount ? count : bucketBoundary(bucket + 1, intermediate, bucketCount);
    for (int i = start; i < end; i++) {
      if (isFiniteValue(values, integralValues, i)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the participant value at {@code index} is finite: integral columns always are, and a
   * floating-point value qualifies when it is neither NaN nor infinite. Such a value can serve as a
   * triangle vertex and is representable as an exact decimal.
   */
  private static boolean isFiniteValue(double[] values, long[] integralValues, int index) {
    return integralValues != null || Double.isFinite(values[index]);
  }

  /** Score candidates against the next bucket average, keeping the first candidate on ties. */
  private static int selectBucketPoint(
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
    if (end - start == 1) {
      return start;
    }
    if (integralValues != null) {
      try {
        return argMaxIntegralArea(
            times,
            integralValues,
            start,
            end,
            anchorTime,
            integralAnchor,
            nextTimes,
            nextIntegralValues,
            nextStart,
            nextEnd);
      } catch (ArithmeticException overflow) {
        // A difference, sum or cross-product can exceed the long range.
      }
    } else {
      int pick =
          argMaxFloatingArea(
              times,
              values,
              start,
              end,
              anchorTime,
              anchorValue,
              nextTimes,
              nextValues,
              nextStart,
              nextEnd);
      if (pick != NOT_SELECTED) {
        return pick;
      }
    }
    return argMaxAreaExactly(
        times,
        values,
        integralValues,
        start,
        end,
        anchorTime,
        anchorValue,
        integralAnchor,
        nextTimes,
        nextValues,
        nextIntegralValues,
        nextStart,
        nextEnd);
  }

  /** Returns NOT_SELECTED if rounding could change which candidate has the largest area. */
  private static int argMaxFloatingArea(
      long[] times,
      double[] values,
      int start,
      int end,
      long anchorTime,
      double anchorValue,
      long[] nextTimes,
      double[] nextValues,
      int nextStart,
      int nextEnd) {
    double sumTime = 0;
    double sumValue = 0;
    double sumAbsTime = 0;
    double sumAbsValue = 0;
    int finiteCount = 0;
    for (int i = nextStart; i < nextEnd; i++) {
      if (Double.isFinite(nextValues[i])) {
        double time = differenceAsDouble(nextTimes[i], anchorTime);
        double value = nextValues[i] - anchorValue;
        sumTime += time;
        sumValue += value;
        sumAbsTime += Math.abs(time);
        sumAbsValue += Math.abs(value);
        finiteCount++;
      }
    }
    if (finiteCount == 0) {
      return start;
    }

    // Every determinant has the same denominator. Keeping the sums avoids rounding the two
    // coordinates of the mean separately. Bound cancellation by summing absolute terms: each
    // sum has at most finiteCount roundings, plus the coordinate differences and determinant.
    // The factor of two also covers rounding in the bound itself.
    double errorUlps = 2.0 * (finiteCount + 8);
    int pick = start;
    double bestUpper = -1;
    double bestLower = -1;
    double runnerUpUpper = -1;
    for (int i = start; i < end; i++) {
      if (!Double.isFinite(values[i])) {
        continue;
      }
      double time = differenceAsDouble(times[i], anchorTime);
      double value = values[i] - anchorValue;
      double area = Math.abs(sumTime * value - time * sumValue);
      double scale = sumAbsTime * Math.abs(value) + Math.abs(time) * sumAbsValue;
      double error = scale == 0 ? 0 : errorUlps * Math.ulp(scale);
      double upper = area + error;
      if (!Double.isFinite(upper)) {
        return NOT_SELECTED;
      }
      if (upper > bestUpper) {
        runnerUpUpper = bestUpper;
        bestUpper = upper;
        bestLower = area - error;
        pick = i;
      } else {
        runnerUpUpper = Math.max(runnerUpUpper, upper);
      }
    }
    if (runnerUpUpper < 0 || bestUpper == 0 || bestLower > runnerUpUpper) {
      return pick;
    }
    return NOT_SELECTED;
  }

  /** Compare scaled triangle areas using the lookahead sums, preserving integer precision. */
  private static int argMaxIntegralArea(
      long[] times,
      long[] values,
      int start,
      int end,
      long anchorTime,
      long anchorValue,
      long[] nextTimes,
      long[] nextValues,
      int nextStart,
      int nextEnd) {
    long sumTime = 0;
    long sumValue = 0;
    for (int i = nextStart; i < nextEnd; i++) {
      sumTime = Math.addExact(sumTime, Math.subtractExact(nextTimes[i], anchorTime));
      sumValue = Math.addExact(sumValue, Math.subtractExact(nextValues[i], anchorValue));
    }
    int pick = start;
    long maxArea = 0;
    for (int i = start; i < end; i++) {
      long value = Math.subtractExact(values[i], anchorValue);
      long time = Math.subtractExact(times[i], anchorTime);
      long determinant =
          Math.subtractExact(
              Math.multiplyExact(sumTime, value), Math.multiplyExact(time, sumValue));
      // abs(MIN_VALUE) represents 2^63 when compared as an unsigned long.
      long area = Math.abs(determinant);
      if (Long.compareUnsigned(area, maxArea) > 0) {
        maxArea = area;
        pick = i;
      }
    }
    return pick;
  }

  /**
   * Exact fallback for overflow and ambiguous floating-point comparisons. Multiplying all areas by
   * the lookahead point count lets us use sums instead of dividing by that count, keeping the
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
      if (!isFiniteValue(nextValues, nextIntegralValues, i)) {
        continue;
      }
      sumTime = sumTime.add(BigDecimal.valueOf(nextTimes[i]).subtract(timeOrigin));
      sumValue = sumValue.add(exactValue(nextValues, nextIntegralValues, i).subtract(valueOrigin));
    }
    int pick = start;
    BigDecimal maxArea = null;
    for (int i = start; i < end; i++) {
      if (!isFiniteValue(values, integralValues, i)) {
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
    private static final long[] EMPTY_LONGS = new long[0];
    private static final double[] EMPTY_DOUBLES = new double[0];

    private final BufferMemoryBudget memoryBudget;
    private long[] times = EMPTY_LONGS;
    private double[] values = EMPTY_DOUBLES;
    private long[] integralValues;
    private int size;
    private int onlyFiniteIndex = NOT_SELECTED;

    PointBuffer(boolean integral, BufferMemoryBudget memoryBudget) {
      this.memoryBudget = memoryBudget;
      if (memoryBudget != null) {
        memoryBudget.reserve(INSTANCE_SIZE);
      }
      integralValues = integral ? EMPTY_LONGS : null;
    }

    private static long arrayBytes(int capacity, boolean integral) {
      if (capacity == 0) {
        // Empty buffers share their backing arrays until the first eligible point arrives.
        return 0;
      }
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

    /**
     * Records the index of a closed bucket's sole finite candidate, if any. Recomputed after
     * removing the partition's final point from the bucket.
     */
    private boolean indexFinitePoints() {
      onlyFiniteIndex = NOT_SELECTED;
      for (int i = 0; i < size; i++) {
        if (Double.isFinite(values[i])) {
          if (onlyFiniteIndex != NOT_SELECTED) {
            onlyFiniteIndex = NOT_SELECTED;
            return true;
          }
          onlyFiniteIndex = i;
        }
      }
      return onlyFiniteIndex != NOT_SELECTED;
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
      int newCapacity =
          size == 0
              ? INITIAL_CAPACITY
              : (int) Math.min((long) times.length * 2, MAX_BUFFERED_POINTS);
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
     * Rejects a null or repeated timestamp in the partition. Runs at the partition input boundary,
     * before per-column {@code NULL} filtering and before window expansion, so a row shared by
     * several overlapping windows is still a single input row.
     */
    protected final long readTimestamp(Record input) {
      if (input.isNull(0)) {
        throw new SemanticException(QueryMessages.TIME_COLUMN_SHOULD_NOT_BE_NULL);
      }
      long time = input.getLong(0);
      timestampChecker.check(time);
      return time;
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
  private static final class TargetCountLTTBDataProcessor extends AbstractLTTBDataProcessor
      implements BatchedTableFunctionDataProcessor {
    private final long n;
    private final PointBuffer[] buffers;
    private final BufferMemoryBudget memoryBudget;
    private boolean hasRows;
    private int[][] selections;
    private int[] selectedIndexes;
    private int rowCount;
    private int nextRow;

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
      long time = readTimestamp(input);
      capturePartitionValues(input);
      collect(input, time, buffers);
    }

    @Override
    public boolean finishBatch(List<ColumnBuilder> builders, BooleanSupplier blockFull) {
      if (!hasRows) {
        return true;
      }
      if (selections == null) {
        prepareSelections();
      }
      while (nextRow < rowCount) {
        builders.get(0).writeLong(0L);
        for (int i = 0; i < participantColumns.length; i++) {
          selectedIndexes[i] =
              nextRow < selections[i].length ? selections[i][nextRow] : NOT_SELECTED;
        }
        writeRow(builders, 1, buffers, selectedIndexes);
        nextRow++;
        if (blockFull.getAsBoolean()) {
          break;
        }
      }
      return nextRow == rowCount;
    }

    private void prepareSelections() {
      long selectionBytes =
          RamUsageEstimator.sizeOfObjectArray(participantColumns.length)
              + RamUsageEstimator.sizeOfIntArray(participantColumns.length);
      for (PointBuffer buffer : buffers) {
        selectionBytes += RamUsageEstimator.sizeOfIntArray((int) Math.min(buffer.size, n));
      }
      memoryBudget.reserve(selectionBytes);
      selections = new int[participantColumns.length][];
      selectedIndexes = new int[participantColumns.length];
      for (int i = 0; i < participantColumns.length; i++) {
        PointBuffer buffer = buffers[i];
        selections[i] =
            selectTargetCount(buffer.times, buffer.values, buffer.integralValues, buffer.size, n);
        rowCount = Math.max(rowCount, selections[i].length);
      }
    }

    @Override
    public void beforeDestroy() {
      Arrays.fill(buffers, null);
      selections = null;
      selectedIndexes = null;
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
    final WindowState[] nextFinite;
    protected final long endExclusive;

    private WindowState(PointBuffer[] buffers, long endExclusive) {
      this.buffers = buffers;
      this.nextFinite = new WindowState[buffers.length];
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
      extends AbstractLTTBDataProcessor implements BatchedTableFunctionDataProcessor {
    private static final long WINDOW_INSTANCE_SIZE =
        Math.max(
            RamUsageEstimator.shallowSizeOfInstance(TimeWindowState.class),
            RamUsageEstimator.shallowSizeOfInstance(CountWindowState.class));

    protected final long size;
    protected final long slide;
    protected final Deque<W> activeWindows = new ArrayDeque<>();
    private final Deque<W> pendingWindows = new ArrayDeque<>();
    // Closed buckets with finite points, in window order for each column.
    private final WindowState[] firstPendingFinite;
    private final WindowState[] lastPendingFinite;
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
    private Record pendingInput;
    private long pendingTime;
    private WindowRow[][] outputRows;
    private int outputRowCount;
    private int nextOutputRow;

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
      long referenceArrayBytes = RamUsageEstimator.sizeOfObjectArray(columnCount);
      // Each of the five remembered points has a presence flag, time, and floating and integral
      // values. The last eligible point also keeps its position. Four arrays link columns to
      // windows.
      long longArrayBytes = RamUsageEstimator.sizeOfLongArray(columnCount);
      long pointStateBytes =
          RamUsageEstimator.sizeOfBooleanArray(columnCount)
              + 2 * longArrayBytes
              + RamUsageEstimator.sizeOfDoubleArray(columnCount);
      memoryBudget.reserve(5 * pointStateBytes + longArrayBytes + 4 * referenceArrayBytes);
      this.firstPendingFinite = new WindowState[columnCount];
      this.lastPendingFinite = new WindowState[columnCount];
      this.windowMetadataBytes = WINDOW_INSTANCE_SIZE + 4 * referenceArrayBytes;
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
      for (int i = 0; i < participantColumns.length; i++) {
        if (firstEligibleWindows[i] == window) {
          firstEligibleWindows[i] = null;
        }
        if (lastRowWindows[i] == window) {
          lastRowWindows[i] = null;
        }
        if (firstPendingFinite[i] == window) {
          firstPendingFinite[i] = window.nextFinite[i];
          if (firstPendingFinite[i] == null) {
            lastPendingFinite[i] = null;
          }
        }
        window.buffers[i].release();
      }
      memoryBudget.release(windowMetadataBytes);
    }

    @Override
    public final void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      processInput(input, properColumnBuilders, null);
      drainOutput(properColumnBuilders, () -> false);
    }

    @Override
    public final void processInput(Record input, List<ColumnBuilder> builders, IoTDBLocal local) {
      pendingTime = readTimestamp(input);
      capturePartitionValues(input);
      closeWindows(position(pendingTime));
      // The iterator stays on this record until drainOutput finishes. Release ready windows before
      // opening the windows for this row, so the old and new buffers need not fit simultaneously.
      pendingInput = input;
    }

    protected abstract long position(long time);

    protected abstract void processRecord(Record input, long time);

    protected final boolean hasParticipantValue(Record input) {
      for (ParticipantColumn column : participantColumns) {
        if (!column.isNull(input)) {
          return true;
        }
      }
      return false;
    }

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
    private void closeWindows(long position) {
      while (!activeWindows.isEmpty() && activeWindows.peekFirst().endExclusive <= position) {
        W window = activeWindows.removeFirst();
        if (canEmitAnyRow(window)) {
          pendingWindows.addLast(window);
          indexFiniteBuckets(window);
        } else {
          // A closed window that holds no bucket and reports no F can never emit a row, and L is
          // never its own row because L was collected into every bucket that contains it. Queueing
          // it would let it hold a place in front of later windows — and its buffers — until the
          // partition ends, so release it now.
          releaseWindow(window);
        }
      }
    }

    private void indexFiniteBuckets(WindowState window) {
      for (int i = 0; i < participantColumns.length; i++) {
        window.nextFinite[i] = null;
        if (window.buffers[i].indexFinitePoints()) {
          if (lastPendingFinite[i] == null) {
            firstPendingFinite[i] = window;
          } else {
            lastPendingFinite[i].nextFinite[i] = window;
          }
          lastPendingFinite[i] = window;
        }
      }
    }

    /** Whether {@code window} can still contribute at least one output row. */
    private boolean canEmitAnyRow(W window) {
      for (int i = 0; i < participantColumns.length; i++) {
        if (window == firstEligibleWindows[i] || !window.buffers[i].isEmpty()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public final boolean finishBatch(List<ColumnBuilder> builders, BooleanSupplier blockFull) {
      if (!finishing) {
        moveLastEligibleOutOfBuckets();
      }
      return drainOutput(builders, blockFull);
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
      // Removing L can turn a formerly finite lookahead bucket into an empty or non-finite one.
      Arrays.fill(firstPendingFinite, null);
      Arrays.fill(lastPendingFinite, null);
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
        indexFiniteBuckets(window);
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
      pendingInput = null;
      outputRows = null;
      activeWindows.clear();
      pendingWindows.clear();
      Arrays.fill(firstPendingFinite, null);
      Arrays.fill(lastPendingFinite, null);
      Arrays.fill(firstEligibleWindows, null);
      Arrays.fill(lastRowWindows, null);
      // Also covers a reservation made by a window whose construction failed at the limit.
      memoryBudget.reservedBytes = 0;
    }

    @Override
    public final boolean drainOutput(List<ColumnBuilder> builders, BooleanSupplier blockFull) {
      while (true) {
        W window = pendingWindows.peekFirst();
        if (window == null || !isReady(window)) {
          if (pendingInput == null) {
            return true;
          }
          processRecord(pendingInput, pendingTime);
          pendingInput = null;
          continue;
        }
        if (outputRows == null) {
          selectOutputRows(window);
        }
        boolean full = false;
        while (nextOutputRow < outputRowCount && !full) {
          window.writeWindowColumns(builders);
          writeWindowRow(builders, window.getWindowColumnCount(), outputRows, nextOutputRow++);
          full = blockFull.getAsBoolean();
        }
        if (nextOutputRow == outputRowCount) {
          releaseWindow(pendingWindows.removeFirst());
          outputRows = null;
        }
        if (full) {
          return pendingInput == null
              && (pendingWindows.isEmpty() || !isReady(pendingWindows.peekFirst()));
        }
      }
    }

    /**
     * Whether the first pending window can be emitted. A window must wait while moving {@code L}
     * into its own bucket could change its output or the lookahead used to choose its sample.
     *
     * <p>A column with fewer than two eligible points has no {@code L}: the only point is {@code F}
     * and was never collected into a bucket. {@link #lastEligiblePositions} still holds its default
     * value then, so it must not be consulted — position {@code 0} would otherwise match the first
     * window and stall every later window until the partition ends.
     */
    private boolean isReady(W window) {
      if (finishing) {
        return true;
      }
      for (int i = 0; i < participantColumns.length; i++) {
        if (hasLastEligible[i] && containsPosition(window, lastEligiblePositions[i])) {
          // The last window containing L reports it. Earlier overlapping windows drop it.
          // Compare offsets so the next window start need not fit in a long.
          long windowStart = window.endExclusive - size;
          boolean shared = lastEligiblePositions[i] - windowStart >= slide;
          int candidateCount = window.buffers[i].size();
          // Dropping a trailing non-finite point leaves the pick and anchor unchanged if another
          // candidate remains. In L's own window only a singleton gives the same output either way.
          boolean sameOutput =
              shared
                  ? candidateCount > 1 && !Double.isFinite(lastEligibleValues[i])
                  : candidateCount == 1;
          if (!sameOutput) {
            return false;
          }
        }
        // A sole finite candidate wins for every possible C.
        if (firstPendingFinite[i] != window || window.buffers[i].onlyFiniteIndex != NOT_SELECTED) {
          continue;
        }
        WindowState lookahead = window.nextFinite[i];
        if (lookahead == null) {
          return false;
        }
        // Moving a finite L out of a multi-point average can change C. If L is the only
        // finite point, it is both the current average and the final fallback.
        if (hasLastEligible[i]
            && containsPosition(lookahead, lastEligiblePositions[i])
            && Double.isFinite(lastEligibleValues[i])
            && lookahead.buffers[i].onlyFiniteIndex == NOT_SELECTED) {
          return false;
        }
      }
      return true;
    }

    /** A one-point lookahead when {@code C} is the last finite point. */
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

    private void selectOutputRows(W window) {
      // Selection advances the anchors. Keep the selected cells when a block ends mid-window.
      outputRows = new WindowRow[participantColumns.length][];
      nextOutputRow = 0;
      outputRowCount = 0;
      for (int i = 0; i < participantColumns.length; i++) {
        outputRows[i] = selectWindowRows(window, i);
        outputRowCount = Math.max(outputRowCount, outputRows[i].length);
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
      if (firstPendingFinite[column] != window) {
        return WindowRow.of(bucket, 0);
      }
      if (bucket.onlyFiniteIndex != NOT_SELECTED) {
        return updateAnchor(bucket, bucket.onlyFiniteIndex, column);
      }
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

      WindowState lookaheadWindow = window.nextFinite[column];
      PointBuffer lookahead;
      if (lookaheadWindow != null) {
        lookahead = lookaheadWindow.buffers[column];
      } else if (finishing && hasLastFinite[column]) {
        // No later bucket carries a finite point: C is the last finite eligible point.
        lookahead = singlePoint(lastFiniteTimes[column], lastFiniteValues[column], column);
      } else {
        return WindowRow.of(bucket, 0);
      }

      int pick =
          selectBucketPoint(
              bucket.times,
              bucket.values,
              bucket.integralValues,
              0,
              bucket.size(),
              anchorTime,
              anchorValue,
              integralAnchor,
              lookahead.times,
              lookahead.values,
              lookahead.integralValues,
              0,
              lookahead.size());
      if (!isFiniteValue(bucket.values, bucket.integralValues, pick)) {
        // The bucket has no finite candidate; return its earliest point and keep A unchanged.
        return WindowRow.of(bucket, 0);
      }
      return updateAnchor(bucket, pick, column);
    }

    private WindowRow updateAnchor(PointBuffer bucket, int index, int column) {
      WindowRow row = WindowRow.of(bucket, index);
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
    protected long position(long time) {
      return time;
    }

    @Override
    protected void processRecord(Record input, long time) {
      if (!windowsExhausted) {
        long firstCandidateStart = getFirstWindowStart(time);
        if (!nextWindowStartInitialized || nextWindowStart < firstCandidateStart) {
          nextWindowStart = firstCandidateStart;
          nextWindowStartInitialized = true;
        }

        if (!windowsExhausted && nextWindowStart <= time) {
          if (time > Long.MAX_VALUE - size) {
            // Check the latest aligned end even when this row has no values.
            long lastStart = time - (time - nextWindowStart) % slide;
            if (lastStart > Long.MAX_VALUE - size) {
              throw windowBoundaryOverflow();
            }
          }
          // A later eligible row opens any still-relevant windows on the same grid.
          if (!hasParticipantValue(input)) {
            return;
          }
        }
        while (!windowsExhausted && nextWindowStart <= time) {
          activeWindows.addLast(
              new TimeWindowState(newWindowBuffers(), nextWindowStart, nextWindowStart + size));
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
    private long nextWindowIndex = 0;

    private CountWindowLTTBDataProcessor(
        long size,
        long slide,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns,
        long maxBufferedBytes) {
      super(size, slide, partitionTypes, participantColumns, maxBufferedBytes);
    }

    @Override
    protected long position(long time) {
      return rowCount;
    }

    @Override
    protected void processRecord(Record input, long time) {
      long position = rowCount++;
      long lastIndex = position / slide;
      if (lastIndex * slide > Long.MAX_VALUE - size) {
        throw new SemanticException(
            CommonMessages.EXCEPTION_LTTB_COUNT_WINDOW_BOUNDARIES_EXCEED_THE_INT64_RANGE_5BC980CA);
      }

      if (nextWindowIndex <= lastIndex) {
        if (!hasParticipantValue(input)) {
          return;
        }
        // A NULL prefix may span many starts. Materialize only windows still containing this row,
        // keeping their indexes on the original grid (including windows that emitted no rows).
        long firstIndex = position < size ? 0 : (position - size) / slide + 1;
        nextWindowIndex = Math.max(nextWindowIndex, firstIndex);
        while (nextWindowIndex <= lastIndex) {
          long end = nextWindowIndex * slide + size;
          activeWindows.addLast(new CountWindowState(newWindowBuffers(), end, nextWindowIndex++));
        }
      }

      collectIntoWindows(position, input, time);
    }
  }
}
