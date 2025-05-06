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

package org.apache.iotdb.library.match;


import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
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

import java.util.*;


//from TABLE(sensor_match(table => s1, pattern => '[(1,1),(2,2),(3,1)]', threshold => 0.5))
//Time, s1, selected, dtwValue
//1,          1,             0,           0.99
//2,          2,             1,           0.1
//3,          3,             1,           0.1
//3,          3,             2,           0.01
//4,          4,             2,           0.01

public class UDTFPatternMatch implements TableFunction {
    private final String TBL_PARAM = "TABLE";
    private final String PATTERN_PARAM = "PATTERN";
    private final String THRESHOLD_PARAM = "THRESHOLD";

    // TODO 似乎没有参数校验

    @Override
    public List<ParameterSpecification> getArgumentsSpecifications(){
        return Arrays.asList(
                TableParameterSpecification.builder().name(TBL_PARAM).passThroughColumns().build(), // TODO 这里是否需要passthroughColumns
                ScalarParameterSpecification.builder().name(PATTERN_PARAM).type(Type.STRING).build(),
                ScalarParameterSpecification.builder().name(THRESHOLD_PARAM).type(Type.FLOAT).build()
        );
    }

    @Override
    public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

        // 组织输出的table的元数据
        TableArgument tableArgument = (TableArgument) arguments.get(TBL_PARAM);
        DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
        for (int i = 0; i < tableArgument.getFieldNames().size(); i++) {
            Optional<String> fieldName = tableArgument.getFieldNames().get(i);
            schemaBuilder.addField(fieldName, tableArgument.getFieldTypes().get(i));
        }
        schemaBuilder.addField("selected", Type.INT32);
        schemaBuilder.addField("dtwValue", Type.FLOAT);

        // 这个是参数传递的
        MapTableFunctionHandle handle = new MapTableFunctionHandle.Builder()
                .addProperty(PATTERN_PARAM, ((ScalarArgument) arguments.get(PATTERN_PARAM)).getValue())
                .addProperty(THRESHOLD_PARAM, ((ScalarArgument) arguments.get(THRESHOLD_PARAM)).getValue())
                .build();

        return TableFunctionAnalysis.builder().properColumnSchema(schemaBuilder.build())
                .requiredColumns(TBL_PARAM, Collections.singletonList(0))
                .handle(handle)
                .build();
    }

    @Override
    public TableFunctionHandle createTableFunctionHandle() { // TODO 似乎是用于参数传递的？文档可能需要更新
        return new MapTableFunctionHandle();
    }

    @Override
    public TableFunctionProcessorProvider getProcessorProvider(
            TableFunctionHandle tableFunctionHandle) {
        return new TableFunctionProcessorProvider() {

            @Override
            public TableFunctionDataProcessor getDataProcessor() {
                return new TableFunctionDataProcessor() {
                    private final String pattern =
                            (String) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(PATTERN_PARAM);
                    private final float threshold =
                            (float) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(THRESHOLD_PARAM);

                    @Override
                    public void process(Record input, List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {

                    }

                    @Override
                    public void finish(List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {

                    }
                }
            }
        }
    }
}
