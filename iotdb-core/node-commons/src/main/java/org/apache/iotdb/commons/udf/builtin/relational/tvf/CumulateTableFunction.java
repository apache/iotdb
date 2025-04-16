package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class CumulateTableFunction implements TableFunction {

  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String STEP_PARAMETER_NAME = "STEP";
  private static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .rowSemantics()
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("time")
            .build(),
        ScalarParameterSpecification.builder().name(SIZE_PARAMETER_NAME).type(Type.INT64).build(),
        ScalarParameterSpecification.builder().name(STEP_PARAMETER_NAME).type(Type.INT64).build(),
        ScalarParameterSpecification.builder()
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // size must be an integral multiple of step.
    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    long step = (long) ((ScalarArgument) arguments.get(STEP_PARAMETER_NAME)).getValue();

    if (step > size || size % step != 0) {
      throw new UDFException(
          "Cumulative table function requires size must be an integral multiple of step.");
    }

    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    String expectedFieldName =
        (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue();
    int requiredIndex =
        findColumnIndex(tableArgument, expectedFieldName, Collections.singleton(Type.TIMESTAMP));
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder()
            .addField("window_start", Type.TIMESTAMP)
            .addField("window_end", Type.TIMESTAMP)
            .build();

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(requiredIndex))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new CumulateDataProcessor(
            (Long) ((ScalarArgument) arguments.get(ORIGIN_PARAMETER_NAME)).getValue(),
            (Long) ((ScalarArgument) arguments.get(STEP_PARAMETER_NAME)).getValue(),
            (Long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue());
      }
    };
  }

  private static class CumulateDataProcessor implements TableFunctionDataProcessor {

    private final long step;
    private final long size;
    private final long start;
    private long curIndex = 0;

    public CumulateDataProcessor(long startTime, long step, long size) {
      this.step = step;
      this.size = size;
      this.start = startTime;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // find the first windows
      long timeValue = input.getLong(0);
      long window_start = (timeValue - start) / size * size;
      long steps = step;
      while (window_start <= timeValue && window_start + steps <= window_start + size) {
        if (window_start + steps > timeValue) {
          properColumnBuilders.get(0).writeLong(window_start);
          properColumnBuilders.get(1).writeLong(window_start + steps);
          passThroughIndexBuilder.writeLong(curIndex);
        }
        steps += step;
      }
      curIndex++;
    }
  }
}
