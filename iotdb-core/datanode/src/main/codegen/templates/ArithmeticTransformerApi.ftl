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
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/transformer/ArithmeticTransformerApi.java" />
package org.apache.iotdb.db.queryengine.transformation.dag.transformer;

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
<#list mathematicalOperator.binaryOperators as operator>
    <#list mathematicalDataType.types as first>
        <#list mathematicalDataType.types as second>
            <#if first.instance != "DATE" && second.instance != "DATE" && first.instance != "TIMESTAMP" && second.instance != "TIMESTAMP">
                import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer;
            </#if>
            <#if (first.instance == "DATE" || first.instance == "TIMESTAMP") && (second.instance == "INT" || second.instance == "LONG")>
                <#if operator.name == "Addition" || operator.name == "Subtraction">
                    import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer;
                </#if>
            </#if>
            <#if (second.instance == "DATE" || second.instance == "TIMESTAMP") && (first.instance == "INT" || first.instance == "LONG")>
                <#if operator.name == "Addition">
                    import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer;
                </#if>
            </#if>
        </#list>
    </#list>
</#list>
<#list mathematicalDataType.types as type>
    <#if type.instance != "DATE">
        import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.${type.type?replace('Type','')}NegationTransformer;
    </#if>
</#list>

import org.apache.tsfile.enums.TSDataType;

public class ArithmeticTransformerApi {

<#list mathematicalOperator.binaryOperators as operator>
    <#assign opLower = operator.operator?lower_case>
    public static Transformer get${operator.name}Transformer(
    LayerReader leftReader, LayerReader rightReader, TSDataType leftType, TSDataType rightType, java.time.ZoneId zoneId) {
    switch (leftType) {
    <#list mathematicalDataType.types as first>
        <#if first.instance == "INT">
            case INT32:
        <#elseif first.instance == "LONG">
            case INT64:
        <#elseif first.instance == "DATE">
            case DATE:
        <#elseif first.instance == "TIMESTAMP">
            case TIMESTAMP:
        <#else>
            case ${first.instance}:
        </#if>
        return get${operator.name}${first.type?replace('Type','')}Transformer(leftReader, rightReader, rightType, zoneId);
    </#list>
    default:
    throw new UnsupportedOperationException("Unsupported data type: " + leftType);
    }
    }

    <#list mathematicalDataType.types as first>
        private static Transformer get${operator.name}${first.type?replace('Type','')}Transformer(
        LayerReader leftReader, LayerReader rightReader, TSDataType rightType, java.time.ZoneId zoneId) {
        switch (rightType) {
        <#list mathematicalDataType.types as second>
            <#if first.instance != "DATE" && second.instance != "DATE" && first.instance != "TIMESTAMP" && second.instance != "TIMESTAMP">
                <#if second.instance == "INT">
                    case INT32:
                <#elseif second.instance == "LONG">
                    case INT64:
                <#else>
                    case ${second.instance}:
                </#if>
                return new ${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer(leftReader, rightReader);
            </#if>
            <#if (first.instance == "DATE" || first.instance == "TIMESTAMP") && (second.instance == "INT" || second.instance == "LONG")>
                <#if operator.name == "Addition" || operator.name == "Subtraction">
                    <#if second.instance == "INT">
                        case INT32:
                    <#else>
                        case INT64:
                    </#if>
                    return new ${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer(leftReader, rightReader, zoneId);
                </#if>
            </#if>
            <#if (second.instance == "DATE" || second.instance == "TIMESTAMP") && (first.instance == "INT" || first.instance == "LONG")>
                <#if operator.name == "Addition">
                    <#if second.instance == "DATE">
                        case DATE:
                    <#else>
                        case TIMESTAMP:
                    </#if>
                    return new ${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}Transformer(leftReader, rightReader, zoneId);
                </#if>
            </#if>
        </#list>
        default:
        throw new UnsupportedOperationException("Unsupported data type: " + rightType);
        }
        }
    </#list>

</#list>
public static Transformer getNegationTransformer(LayerReader layerReader, TSDataType dataType) {
switch (dataType) {
<#list mathematicalDataType.types as type>
    <#if type.instance != "DATE">
        <#if type.instance == "INT">
            case INT32:
        <#elseif type.instance == "LONG">
            case INT64:
        <#else>
            case ${type.instance}:
        </#if>
        return new ${type.type?replace('Type','')}NegationTransformer(layerReader);
    </#if>
</#list>
default:
throw new UnsupportedOperationException("Unsupported data type: " + dataType);
}
}
}


