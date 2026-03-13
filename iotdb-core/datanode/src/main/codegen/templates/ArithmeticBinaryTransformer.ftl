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
<#list mathematicalOperator.binaryOperators as operator>
    <#list mathematicalDataType.types as first>
        <#list mathematicalDataType.types as second>
            <#assign className = "${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer">
        <#-- DATE/TIMESTAMP operations -->
            <#if (first.instance == "DATE" || first.instance == "TIMESTAMP") && (second.instance == "INT" || second.instance == "LONG")>
                <#if operator.name == "Addition" || operator.name == "Subtraction">
                    <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/transformer/binary/${className}.java" />
                    package org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary;

                    import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
                    import org.apache.iotdb.db.exception.query.QueryProcessException;
                    import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
                    import org.apache.iotdb.db.utils.DateTimeUtils;

                    import org.apache.tsfile.block.column.Column;
                    import org.apache.tsfile.block.column.ColumnBuilder;
                    import org.apache.tsfile.enums.TSDataType;
                    import org.apache.tsfile.utils.DateUtils;

                    <#if first.instance == "DATE">
                        import java.time.format.DateTimeParseException;

                        import static org.apache.iotdb.rpc.TSStatusCode.DATE_OUT_OF_RANGE;
                    </#if>
                    import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

                    public class ${className} extends BinaryTransformer {

                    private final java.time.ZoneId zoneId;

                    public ${className}(LayerReader leftReader, LayerReader rightReader, java.time.ZoneId zoneId) {
                    super(leftReader, rightReader);
                    this.zoneId = zoneId;
                    }

                    @Override
                    protected void transformAndCache(
                    Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
                    throws QueryProcessException {
                    ${first.dataType} leftValue = leftValues.get${first.dataType?cap_first}(leftIndex);
                    ${second.dataType} rightValue = rightValues.get${second.dataType?cap_first}(rightIndex);
                    builder.write${first.dataType?cap_first}(transform(leftValue, rightValue, this.zoneId));
                    }

                    @Override
                    protected void checkType() {
                    // do nothing
                    }

                    @Override
                    public TSDataType[] getDataTypes() {
                    <#if first.instance == "DATE">
                        return new TSDataType[] {TSDataType.DATE};
                    <#else>
                        return new TSDataType[] {TSDataType.INT64};
                    </#if>
                    }

                    public static ${first.dataType} transform(${first.dataType} left, ${second.dataType} right, java.time.ZoneId zoneId) {
                    <#if operator.name == "Addition">
                        <#if first.instance == "DATE">
                            try {
                            long timestamp = Math.addExact(
                            DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(left, zoneId)), right);
                            return DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(timestamp, zoneId));
                            } catch (ArithmeticException e) {
                            throw new IoTDBRuntimeException(
                            String.format("long addition overflow: %s + %s", left, right),
                            NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                            } catch (DateTimeParseException e) {
                            throw new IoTDBRuntimeException(
                            "Year must be between 1000 and 9999.", DATE_OUT_OF_RANGE.getStatusCode(), true);
                            }
                        <#else>
                            try {
                            return Math.addExact(left, right);
                            } catch (ArithmeticException e) {
                            throw new IoTDBRuntimeException(
                            String.format("long addition overflow: %s + %s", left, right),
                            NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                            }
                        </#if>
                    <#elseif operator.name == "Subtraction">
                        <#if first.instance == "DATE">
                            try {
                            long timestamp = Math.subtractExact(
                            DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(left, zoneId)), right);
                            return DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(timestamp, zoneId));
                            } catch (ArithmeticException e) {
                            throw new IoTDBRuntimeException(
                            String.format("long subtraction overflow: %s - %s", left, right),
                            NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                            } catch (DateTimeParseException e) {
                            throw new IoTDBRuntimeException(
                            "Year must be between 1000 and 9999.", DATE_OUT_OF_RANGE.getStatusCode(), true);
                            }
                        <#else>
                            try {
                            return Math.subtractExact(left, right);
                            } catch (ArithmeticException e) {
                            throw new IoTDBRuntimeException(
                            String.format("long subtraction overflow: %s - %s", left, right),
                            NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                            }
                        </#if>
                    </#if>
                    }
                    }
                </#if>
            <#elseif (second.instance == "DATE" || second.instance == "TIMESTAMP") && (first.instance == "INT" || first.instance == "LONG")>
                <#if operator.name == "Addition">
                    <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/transformer/binary/${className}.java" />
                    package org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary;

                    import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
                    import org.apache.iotdb.db.exception.query.QueryProcessException;
                    import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
                    import org.apache.iotdb.db.utils.DateTimeUtils;

                    import org.apache.tsfile.block.column.Column;
                    import org.apache.tsfile.block.column.ColumnBuilder;
                    import org.apache.tsfile.enums.TSDataType;
                    import org.apache.tsfile.utils.DateUtils;

                    <#if second.instance == "DATE">
                        import java.time.format.DateTimeParseException;

                        import static org.apache.iotdb.rpc.TSStatusCode.DATE_OUT_OF_RANGE;
                    </#if>
                    import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

                    public class ${className} extends BinaryTransformer {

                    private final java.time.ZoneId zoneId;

                    public ${className}(LayerReader leftReader, LayerReader rightReader, java.time.ZoneId zoneId) {
                    super(leftReader, rightReader);
                    this.zoneId = zoneId;
                    }

                    @Override
                    protected void transformAndCache(
                    Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
                    throws QueryProcessException {
                    ${first.dataType} leftValue = leftValues.get${first.dataType?cap_first}(leftIndex);
                    ${second.dataType} rightValue = rightValues.get${second.dataType?cap_first}(rightIndex);
                    builder.write${second.dataType?cap_first}(transform(leftValue, rightValue, this.zoneId));
                    }

                    @Override
                    protected void checkType() {
                    // do nothing
                    }

                    @Override
                    public TSDataType[] getDataTypes() {
                    <#if second.instance == "DATE">
                        return new TSDataType[] {TSDataType.DATE};
                    <#else>
                        return new TSDataType[] {TSDataType.INT64};
                    </#if>
                    }

                    public static ${second.dataType} transform(${first.dataType} left, ${second.dataType} right, java.time.ZoneId zoneId) {
                    <#if second.instance == "DATE">
                        try {
                        long timestamp = Math.addExact(left,
                        DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(right, zoneId)));
                        return DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(timestamp, zoneId));
                        } catch (ArithmeticException e) {
                        throw new IoTDBRuntimeException(
                        String.format("long addition overflow: %s + %s", left, right),
                        NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                        } catch (DateTimeParseException e) {
                        throw new IoTDBRuntimeException(
                        "Year must be between 1000 and 9999.", DATE_OUT_OF_RANGE.getStatusCode(), true);
                        }
                    <#else>
                        try {
                        return Math.addExact(left, right);
                        } catch (ArithmeticException e) {
                        throw new IoTDBRuntimeException(
                        String.format("long addition overflow: %s + %s", left, right),
                        NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                        }
                    </#if>
                    }
                    }
                </#if>
            <#elseif first.instance != "DATE" && second.instance != "DATE" && first.instance != "TIMESTAMP" && second.instance != "TIMESTAMP">
                <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/transformer/binary/${className}.java" />
                package org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary;

                import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
                import org.apache.iotdb.db.exception.query.QueryProcessException;
                import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

                import org.apache.tsfile.block.column.Column;
                import org.apache.tsfile.block.column.ColumnBuilder;
                import org.apache.tsfile.enums.TSDataType;

                <#if first.dataType == "int" || second.dataType == "int" || first.dataType == "long" || second.dataType == "long">
                    import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;
                </#if>
                <#if operator.name == "Division" || operator.name == "Modulus">
                    import static org.apache.iotdb.rpc.TSStatusCode.DIVISION_BY_ZERO;
                </#if>

                public class ${className} extends BinaryTransformer {

                public ${className}(LayerReader leftReader, LayerReader rightReader) {
                super(leftReader, rightReader);
                }

                @Override
                protected void transformAndCache(
                Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
                throws QueryProcessException {
                ${first.dataType} leftValue = leftValues.get${first.dataType?cap_first}(leftIndex);
                ${second.dataType} rightValue = rightValues.get${second.dataType?cap_first}(rightIndex);
                <#if (first.instance == "DOUBLE" || second.instance == "DOUBLE")>
                    builder.writeDouble(transform(leftValue, rightValue));
                <#elseif (first.instance == "FLOAT" || second.instance == "FLOAT")>
                    builder.writeFloat(transform(leftValue, rightValue));
                <#elseif (first.instance == "LONG" || second.instance == "LONG")>
                    builder.writeLong(transform(leftValue, rightValue));
                <#else>
                    builder.writeInt(transform(leftValue, rightValue));
                </#if>
                }

                @Override
                protected void checkType() {
                // do nothing
                }

                @Override
                public TSDataType[] getDataTypes() {
                <#if (first.instance == "DOUBLE" || second.instance == "DOUBLE")>
                    return new TSDataType[] {TSDataType.DOUBLE};
                <#elseif (first.instance == "FLOAT" || second.instance == "FLOAT")>
                    return new TSDataType[] {TSDataType.FLOAT};
                <#elseif (first.instance == "LONG" || second.instance == "LONG")>
                    return new TSDataType[] {TSDataType.INT64};
                <#else>
                    return new TSDataType[] {TSDataType.INT32};
                </#if>
                }

                public static <#if (first.instance == "DOUBLE" || second.instance == "DOUBLE")>double<#elseif (first.instance == "FLOAT" || second.instance == "FLOAT")>float<#elseif (first.instance == "LONG" || second.instance == "LONG")>long<#else>int</#if> transform(${first.dataType} left, ${second.dataType} right) {
                <#if operator.name == "Addition">
                    <#if (first.dataType == "int" || first.dataType == "long") && (second.dataType == "int" || second.dataType == "long")>
                        try {
                        return Math.addExact(left, right);
                        } catch (ArithmeticException e) {
                        throw new IoTDBRuntimeException(String.format("The result overflow: %s + %s", left, right), NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                        }
                    <#else>
                        return left + right;
                    </#if>
                <#elseif operator.name == "Subtraction">
                    <#if (first.dataType == "int" || first.dataType == "long") && (second.dataType == "int" || second.dataType == "long")>
                        try {
                        return Math.subtractExact(left, right);
                        } catch (ArithmeticException e) {
                        throw new IoTDBRuntimeException(String.format("The result overflow: %s - %s", left, right), NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                        }
                    <#else>
                        return left - right;
                    </#if>
                <#elseif operator.name == "Multiplication">
                    <#if (first.dataType == "int" || first.dataType == "long") && (second.dataType == "int" || second.dataType == "long")>
                        try {
                        return Math.multiplyExact(left, right);
                        } catch (ArithmeticException e) {
                        throw new IoTDBRuntimeException(String.format("The result overflow: %s * %s", left, right), NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
                        }
                    <#else>
                        return left * right;
                    </#if>
                <#elseif operator.name == "Division">
                    if (right == 0) {
                    throw new IoTDBRuntimeException("Division by zero", DIVISION_BY_ZERO.getStatusCode(), true);
                    }
                    <#if (first.instance == "DOUBLE" || second.instance == "DOUBLE")>
                        return left / (double) right;
                    <#elseif (first.instance == "FLOAT" || second.instance == "FLOAT")>
                        return left / (float) right;
                    <#elseif (first.instance == "LONG" || second.instance == "LONG")>
                        return left / right;
                    <#else>
                        return left / right;
                    </#if>
                <#elseif operator.name == "Modulus">
                    if (right == 0) {
                    throw new IoTDBRuntimeException("Modulus by zero", DIVISION_BY_ZERO.getStatusCode(), true);
                    }
                    return left % right;
                </#if>
                }
                }
            </#if>
        </#list>
    </#list>
</#list>


