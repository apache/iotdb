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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.QetchAlgorithm;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model.MatchState;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model.Point;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model.RegexMatchState;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model.Section;
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

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class PatternMatchTableFunction implements TableFunction {
  private static final String TBL_PARAM = "DATA";
  private static final String TIME_COLUMN = "TIME_COL";
  private static final String DATA_COLUMN = "DATA_COL";
  private static final String PATTERN_PARAM = "PATTERN";
  private static final String SMOOTH_PARAM = "SMOOTH";
  private static final String THRESHOLD_PARAM = "THRESHOLD";
  private static final String WIDTH_PARAM = "WIDTH";
  private static final String HEIGHT_PARAM = "HEIGHT";
  private static final String SMOOTH_ON_PATTERN = "SMOOTH_ON_PATTERN";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(TBL_PARAM).passThroughColumns().build(),
        ScalarParameterSpecification.builder()
            .name(TIME_COLUMN)
            .type(Type.STRING)
            .defaultValue("time")
            .build(),
        ScalarParameterSpecification.builder().name(DATA_COLUMN).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(PATTERN_PARAM).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(SMOOTH_PARAM).type(Type.DOUBLE).build(),
        ScalarParameterSpecification.builder().name(THRESHOLD_PARAM).type(Type.DOUBLE).build(),
        ScalarParameterSpecification.builder()
            .name(WIDTH_PARAM)
            .type(Type.DOUBLE)
            .defaultValue(Double.MAX_VALUE)
            .build(),
        ScalarParameterSpecification.builder()
            .name(HEIGHT_PARAM)
            .type(Type.DOUBLE)
            .defaultValue(Double.MAX_VALUE)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SMOOTH_ON_PATTERN)
            .type(Type.BOOLEAN)
            .defaultValue(false)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // calc the index of the column
    TableArgument tableArgument = (TableArgument) arguments.get(TBL_PARAM);
    String expectedTimeName = (String) ((ScalarArgument) arguments.get(TIME_COLUMN)).getValue();
    String expectedDataName = (String) ((ScalarArgument) arguments.get(DATA_COLUMN)).getValue();
    int requiredTimeIndex =
        findColumnIndex(tableArgument, expectedTimeName, ImmutableSet.of(Type.TIMESTAMP));
    int requiredDataIndex =
        findColumnIndex(
            tableArgument,
            expectedDataName,
            ImmutableSet.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE));

    // outputColumnSchema description
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder()
            .addField("match_index", Type.INT32)
            .addField("similarity", Type.DOUBLE)
            .build();

    // this is for transferring the parameters to the processor
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(PATTERN_PARAM, ((ScalarArgument) arguments.get(PATTERN_PARAM)).getValue())
            .addProperty(SMOOTH_PARAM, ((ScalarArgument) arguments.get(SMOOTH_PARAM)).getValue())
            .addProperty(
                THRESHOLD_PARAM, ((ScalarArgument) arguments.get(THRESHOLD_PARAM)).getValue())
            .addProperty(WIDTH_PARAM, ((ScalarArgument) arguments.get(WIDTH_PARAM)).getValue())
            .addProperty(HEIGHT_PARAM, ((ScalarArgument) arguments.get(HEIGHT_PARAM)).getValue())
            .addProperty(
                SMOOTH_ON_PATTERN, ((ScalarArgument) arguments.get(SMOOTH_ON_PATTERN)).getValue())
            .build();

    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requireRecordSnapshot(false)
        .requiredColumns(
            TBL_PARAM,
            Arrays.asList(requiredTimeIndex, requiredDataIndex)) // the 0th column is time
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
    String pattern =
        (String) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(PATTERN_PARAM);
    Double smoothValue =
        (Double) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(SMOOTH_PARAM);
    Double threshold =
        (Double) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(THRESHOLD_PARAM);
    Double widthLimit =
        (Double) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(WIDTH_PARAM);
    Double heightLimit =
        (Double) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(HEIGHT_PARAM);
    boolean isPatternFromOrigin =
        (Boolean) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(SMOOTH_ON_PATTERN);

    QetchAlgorithm qetchAlgorithm = new QetchAlgorithm();
    qetchAlgorithm.setThreshold(threshold);
    qetchAlgorithm.setSmoothValue(smoothValue);
    qetchAlgorithm.setHeightLimit(heightLimit);
    qetchAlgorithm.setWidthLimit(widthLimit);
    qetchAlgorithm.setIsPatternFromOrigin(isPatternFromOrigin);
    qetchAlgorithm.parsePattern2Automaton(pattern);

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new ShapeMatchDataProcessor(qetchAlgorithm);
      }
    };
  }

  private static class ShapeMatchDataProcessor implements TableFunctionDataProcessor {

    private final QetchAlgorithm qetchAlgorithm;

    public ShapeMatchDataProcessor(QetchAlgorithm qetchAlgorithm) {
      this.qetchAlgorithm = qetchAlgorithm;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {

      double time = input.getLong(0);
      double value = input.getDouble(1);

      qetchAlgorithm.addPoint(new Point(time, value, qetchAlgorithm.getPointNum()));
      if (qetchAlgorithm.hasMatchResult()) {
        outputWindow(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getMatchResult());
      }
      if (qetchAlgorithm.hasRegexMatchResult()) {
        outputWindowRegex(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getRegexMatchResult());
      }
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      qetchAlgorithm.closeNowDataSegment();
      if (qetchAlgorithm.hasMatchResult()) {
        outputWindow(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getMatchResult());
      }
      if (qetchAlgorithm.hasRegexMatchResult()) {
        outputWindowRegex(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getRegexMatchResult());
      }
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        MatchState matchResult) {
      int matchResultID = qetchAlgorithm.getMatchResultID();
      for (int i = 0; i < matchResult.getDataSectionList().size(); i++) {
        for (int j = i == 0 ? 0 : 1;
            j < matchResult.getDataSectionList().get(i).getPoints().size();
            j++) {
          passThroughIndexBuilder.writeLong(
              matchResult.getDataSectionList().get(i).getPoints().get(j).index);
          properColumnBuilders.get(0).writeInt(matchResultID);
          properColumnBuilders.get(1).writeDouble(matchResult.getMatchValue());
        }
      }

      // after the process, the result of qetchAlgorthm will be empty
      qetchAlgorithm.matchResultClear();
      if (qetchAlgorithm.checkNextMatchResult()) {
        outputWindow(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getMatchResult());
      }
    }

    private void outputWindowRegex(
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        RegexMatchState matchResult) {
      for (RegexMatchState.PathState pathState : matchResult.getMatchResult()) {
        int matchResultID = qetchAlgorithm.getMatchResultID();
        int dataSectionIndex = pathState.getDataSectionIndex();
        List<Section> dataSectionList = matchResult.getDataSectionList();
        for (int i = 0; i <= dataSectionIndex; i++) {
          for (int j = i == 0 ? 0 : 1; j < dataSectionList.get(i).getPoints().size(); j++) {
            passThroughIndexBuilder.writeLong(dataSectionList.get(i).getPoints().get(j).index);
            properColumnBuilders.get(0).writeInt(matchResultID);
            properColumnBuilders.get(1).writeDouble(pathState.getMatchValue());
          }
        }
      }

      // after the process, the result of qetchAlgorthm will be empty
      qetchAlgorithm.matchResultClear();
      if (qetchAlgorithm.checkNextRegexMatchResult()) {
        outputWindowRegex(
            properColumnBuilders, passThroughIndexBuilder, qetchAlgorithm.getRegexMatchResult());
      }
    }
  }
}
