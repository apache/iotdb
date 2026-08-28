package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.FilterTransferTableFunction.FilterTransferDataProcessor.MAX_COUNT_IN_ONE_PARTITION;

public class XCorrTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String PARTITION_TYPES_PROPERTY = "PARTITION_TYPES";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // order by column must only be the time column
    int timeColumnIndex =
        WindowTVFUtils.checkOrderByColumn(arguments, DATA_PARAMETER_NAME, TIMECOL_PARAMETER_NAME);
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);

    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    List<Type> partitionTypes = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();

    // record the partition columns
    for (int partitionIndex : partitionIndexes) {
      Type partitionType = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(partitionType);
      schemaBuilder.addField(
          tableArgument.getFieldNames().get(partitionIndex).get(), partitionType);
    }

    List<Integer> calculationIndexes =
        new ArrayList<>(WindowTVFUtils.getCalculationIndexes(tableArgument, excludedIndexes, null));

    if (calculationIndexes.size() != 2) {
      throw new SemanticException(
          String.format(
              CommonMessages
                  .EXCEPTION_XCORR_REQUIRES_EXACTLY_TWO_CALCULATION_COLUMNS_BUT_FOUND_ARG_2FF8EB0C,
              calculationIndexes.size()));
    }

    // XCorr emits one correlation value per lag; the original time column is used for ordering
    // only and is not part of the result schema.
    String firstColumnName = tableArgument.getFieldNames().get(calculationIndexes.get(0)).get();
    String secondColumnName = tableArgument.getFieldNames().get(calculationIndexes.get(1)).get();
    schemaBuilder.addField(
        String.format("xcorr(%s, %s)", firstColumnName, secondColumnName), Type.DOUBLE);

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(PARTITION_TYPES_PROPERTY, WindowTVFUtils.joinTypes(partitionTypes))
            .build();

    List<Integer> requiredColumns = new ArrayList<>(partitionIndexes);
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(calculationIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
        .handle(handle)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    Type[] partitionTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new XCorrDataProcessor(partitionTypes);
      }
    };
  }

  private static class XCorrDataProcessor implements TableFunctionDataProcessor {

    private static final int INITIAL_CAPACITY = 512;

    private final int partitionColumnCount;
    private final Type[] partitionTypes;
    private final Object[] partitionValues;

    private double[] firstValues;
    private double[] secondValues;
    private int partitionRowCount;

    private XCorrDataProcessor(Type[] partitionTypes) {
      this.partitionTypes = partitionTypes;
      this.partitionColumnCount = partitionTypes.length;
      this.partitionValues = new Object[partitionColumnCount];
      this.firstValues = new double[INITIAL_CAPACITY];
      this.secondValues = new double[INITIAL_CAPACITY];
      this.partitionRowCount = 0;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (partitionRowCount >= MAX_COUNT_IN_ONE_PARTITION) {
        throw new SemanticException(
            CommonMessages.EXCEPTION_FILTER_FUNCTION_ROW_INDEX_EXCEED_MAXIMUM);
      }

      if (partitionRowCount == 0) {
        for (int i = 0; i < partitionColumnCount; i++) {
          partitionValues[i] =
              input.isNull(i) ? null : WindowTVFUtils.readValue(input, i, partitionTypes[i]);
        }
      }

      ensureCapacity(partitionRowCount + 1);
      int firstValueIndex = partitionColumnCount + 1;
      int secondValueIndex = partitionColumnCount + 2;
      // Keep the two series aligned. A null value is represented by NaN and skipped during a pair.
      firstValues[partitionRowCount] = readFiniteValueOrNaN(input, firstValueIndex);
      secondValues[partitionRowCount] = readFiniteValueOrNaN(input, secondValueIndex);
      partitionRowCount++;
    }

    private static double readFiniteValueOrNaN(Record input, int columnIndex) {
      if (input.isNull(columnIndex)) {
        return Double.NaN;
      }
      double value = input.getDouble(columnIndex);
      return Double.isFinite(value) ? value : Double.NaN;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (partitionRowCount == 0) {
        return;
      }

      ColumnBuilder correlationBuilder = properColumnBuilders.get(partitionColumnCount);
      // Emit lags in the documented order: -(n - 1), ..., 0, ..., +(n - 1).
      for (int lag = 1 - partitionRowCount; lag < partitionRowCount; lag++) {
        int firstStart = Math.max(0, lag);
        int secondStart = Math.max(0, -lag);
        int overlapLength = partitionRowCount - Math.abs(lag);
        double correlation = 0.0;
        int validPairCount = 0;

        for (int i = 0; i < overlapLength; i++) {
          double firstValue = firstValues[firstStart + i];
          double secondValue = secondValues[secondStart + i];
          if (Double.isFinite(firstValue) && Double.isFinite(secondValue)) {
            correlation += firstValue * secondValue;
            validPairCount++;
          }
        }

        for (int i = 0; i < partitionColumnCount; i++) {
          WindowTVFUtils.writeValue(
              properColumnBuilders.get(i), partitionValues[i], partitionTypes[i]);
        }
        if (validPairCount == 0) {
          correlationBuilder.appendNull();
        } else {
          correlationBuilder.writeDouble(correlation / validPairCount);
        }
      }
    }

    private void ensureCapacity(int requiredCapacity) {
      if (requiredCapacity <= firstValues.length) {
        return;
      }
      int newCapacity = firstValues.length + (firstValues.length >> 1);
      while (newCapacity < requiredCapacity) {
        newCapacity += newCapacity >> 1;
      }
      firstValues = Arrays.copyOf(firstValues, newCapacity);
      secondValues = Arrays.copyOf(secondValues, newCapacity);
    }
  }
}
