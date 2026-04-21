/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

<@pp.dropOutputFile />
<#list mathematicalDataType.types as type>
    <#assign newType = type.type?replace("Type","")>
    <#assign className = "${newType}NegationTransformer">
    <#if newType != "Date">
        <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/transformer/unary/${className}.java" />
        package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary;

        import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
        import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

        import org.apache.tsfile.block.column.Column;
        import org.apache.tsfile.block.column.ColumnBuilder;
        import org.apache.tsfile.enums.TSDataType;

        import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

        public class ${className} extends UnaryTransformer {

        public ${className}(LayerReader layerReader) {
        super(layerReader);
        }

        @Override
        public TSDataType[] getDataTypes() {
        <#if type.instance == "INT">
            return new TSDataType[] {TSDataType.INT32};
        <#elseif type.instance == "LONG" || type.instance == "TIMESTAMP">
            return new TSDataType[] {TSDataType.INT64};
        <#else>
            return new TSDataType[] {TSDataType.${type.instance}};
        </#if>
        }

        @Override
        protected void transform(Column[] columns, ColumnBuilder builder) {
        int count = columns[0].getPositionCount();
        ${type.dataType}[] values = columns[0].get${type.dataType?cap_first}s();
        boolean[] isNulls = columns[0].isNull();

        for (int i = 0; i < count; i++) {
        if (!isNulls[i]) {
        builder.write${type.dataType?cap_first}(transform(values[i]));
        } else {
        builder.appendNull();
        }
        }
        }

        public static ${type.dataType} transform(${type.dataType} value) {
        <#if type.dataType == "int" || type.dataType == "long">
            if (value == <#if type.dataType == "int">Integer<#else>Long</#if>.MIN_VALUE) {
            throw new IoTDBRuntimeException(String.format("The %s is out of range of ${type.dataType}.", value), NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
            }
        </#if>
        return -value;
        }
        }
    </#if>
</#list>
