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

import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.QetchAlgorthm;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.MatchState;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.Point;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.RegexMatchState;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.Section;
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

// from shape_match(data => t1, col => 's1', pattern => '(1,1),(2,2),(3,1)', smoothValue => 0.5,
// threshold => 0.5)
// 这里还可以再加上结果的范围限制，加速查询（因为UDTF过程当中可能无法获取到下推的谓词）
// 这里加上平滑参数

// input
// Time,       s1,
// 1,          1,
// 2,          2,
// 3,          3,
// 4,          2,
// 5,          1,
// 6,          6,
// 7,          7,
// 8,          8,
// 9,          9,

// output
// Time,       s1,          matchID,    matchValue
// 1,          1,             0,           0.99
// 2,          2,             1,           0.1
// 3,          3,             1,           0.1
// 4,          2,             2,           0.01
// 5,          1,             2,           0.01

public class ShapeMatchTableFunction implements TableFunction {
  private static final String TBL_PARAM = "DATA";
  private static final String COL_PARAM = "COL";
  private static final String PATTERN_PARAM = "PATTERN";
  private static final String SMOOTH_PARAM = "SMOOTH";
  private static final String THRESHOLD_PARAM = "THRESHOLD";
  private static final String WIDTH_PARAM = "WIDTH";
  private static final String HEIGHT_PARAM = "HEIGHT";
  private static final String TYPE_PARAM = "TYPE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    // TODO：这里rowSemantice还是setSemantice不太清楚，因为我需要将数据全部读出来，不论是按行读取还是按partition读取都一样，这里看哪个效率高用哪个
    // TODO: 这里不使用passThroughColumns，是因为我输出的结果是原来列结果的一部分，我在处理的时候不会保留这是第几行的信息，所以可能穿透不太好完成，可以看看是否可以穿透
    return Arrays.asList(
        TableParameterSpecification.builder().name(TBL_PARAM).build(),
        ScalarParameterSpecification.builder().name(COL_PARAM).type(Type.STRING).build(),
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
            .name(TYPE_PARAM)
            .type(Type.STRING)
            .defaultValue("shape")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // check whether the arguments are valid TODO 似乎没有用于参数校验的函数，是要写在analyze里面吗

    // calc the index of the column
    TableArgument tableArgument = (TableArgument) arguments.get(TBL_PARAM);
    String expectedFieldName = (String) ((ScalarArgument) arguments.get(COL_PARAM)).getValue();
    int requiredIndex =
        findColumnIndex(
            tableArgument,
            expectedFieldName,
            ImmutableSet.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE));

    // outputColumnSchema description
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder()
            .addField("time", Type.TIMESTAMP)
            .addField(expectedFieldName, tableArgument.getFieldTypes().get(requiredIndex))
            .addField("matchID", Type.INT32)
            .addField("matchValue", Type.DOUBLE)
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
            .addProperty(TYPE_PARAM, ((ScalarArgument) arguments.get(TYPE_PARAM)).getValue())
            .build();

    // TODO: 这里的requireRecordSnapshot 是要false还是true
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requireRecordSnapshot(false)
        .requiredColumns(TBL_PARAM, Arrays.asList(0, requiredIndex)) // the 0th column is time
        .handle(handle)
        .build();
  }

  // TODO: this is newer function which is not in the manual
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
    String type = (String) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(TYPE_PARAM);

    QetchAlgorthm qetchAlgorthm = new QetchAlgorthm();
    qetchAlgorthm.setThreshold(threshold);
    qetchAlgorthm.setSmoothValue(smoothValue);
    qetchAlgorthm.setHeightLimit(heightLimit);
    qetchAlgorthm.setWidthLimit(widthLimit);
    qetchAlgorthm.setType(type);
    qetchAlgorthm.parsePattern2Automaton(pattern);

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new ShapeMatchDataProcessor(qetchAlgorthm);
      }
    };
  }

  private static class ShapeMatchDataProcessor implements TableFunctionDataProcessor {

    private QetchAlgorthm qetchAlgorthm;

    public ShapeMatchDataProcessor(QetchAlgorthm qetchAlgorthm) {
      this.qetchAlgorthm = qetchAlgorthm;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {

      double time = input.getLong(0);
      double value = input.getDouble(1);

      qetchAlgorthm.addPoint(new Point(time, value));
      if(qetchAlgorthm.hasMatchResult()){
        outputWindow(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getMatchResult());
      }
      if(qetchAlgorthm.hasRegexMatchResult()) {
        outputWindowRegex(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getRegexMatchResult());
      }
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      qetchAlgorthm.closeNowDataSegment();
      if(qetchAlgorthm.hasMatchResult()){
        outputWindow(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getMatchResult());
      }
      if(qetchAlgorthm.hasRegexMatchResult()) {
        outputWindowRegex(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getRegexMatchResult());
      }
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        MatchState matchResult) {
      // TODO fill the result to the output column
      int matchResultID = qetchAlgorthm.getMatchResultID();
      for (Section section : matchResult.getDataSectionList()) {
        for (Point point : section.getPoints()) {
          properColumnBuilders.get(0).writeLong((long) point.x);
          properColumnBuilders.get(1).writeDouble(point.y);
          properColumnBuilders.get(2).writeInt(matchResultID);
          properColumnBuilders.get(3).writeDouble(matchResult.getMatchValue());
        }
      }

      // after the process, the result of qetchAlgorthm will be empty
      qetchAlgorthm.matchResultClear();
      if(qetchAlgorthm.checkNextMatchResult()) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getMatchResult());
      }
    }

    private void outputWindowRegex(
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        RegexMatchState matchResult) {
      // TODO fill the result to the output column
      for (RegexMatchState.PathState pathState : matchResult.getMatchResult()) {
        int matchResultID = qetchAlgorthm.getMatchResultID();
        int dataSectionIndex = pathState.getDataSectionIndex();
        List<Section> dataSectionList = matchResult.getDataSectionList();
        for (int i = 0; i <= dataSectionIndex; i++) {
          for (Point point : dataSectionList.get(i).getPoints()) {
            properColumnBuilders.get(0).writeLong((long) point.x);
            properColumnBuilders.get(1).writeDouble(point.y);
            properColumnBuilders.get(2).writeInt(matchResultID);
            properColumnBuilders.get(3).writeDouble(pathState.getMatchValue());
          }
        }
      }

      // after the process, the result of qetchAlgorthm will be empty
      qetchAlgorthm.matchResultClear();
      if(qetchAlgorthm.checkNextRegexMatchResult()){
        outputWindowRegex(properColumnBuilders, passThroughIndexBuilder, qetchAlgorthm.getRegexMatchResult());
      }
    }
  }
}
