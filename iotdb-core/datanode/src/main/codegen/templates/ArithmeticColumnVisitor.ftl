<@pp.dropOutputFile />
<#list mathematicalOperator.binaryOperators as operator>
<#assign className = "${operator.name}TransformerVisitor">
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/util/visitor/${className}.java" />
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.util.visitor;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
<#list decimalDataTypes.types as first>
<#list decimalDataTypes.types as second>
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.${first.dataType?cap_first}${operator.name}${second.dataType?cap_first}ColumnTransformer;
</#list>
</#list>
import org.apache.tsfile.read.common.type.Type;

public class ${className} implements BinaryTransformerVisitor {

  <#list mathematicalDataType.types as type>
  <#assign newType = type.type?replace("Type", "")>
  <#if type.instance == "DATE" || type.instance == "TIMESTAMP">

  @Override
  public ColumnTransformer visit${newType}(ColumnTransformer left, ColumnTransformer right) {
    <#if operator.name == "Addition" || operator.name == "Subtraction">
    Type returnType = ${operator.name}Resolver.checkConditions(left.getType(), right.getType());
    switch (right.getType().getTypeEnum()) {
      case INT32:
        return new ${type.dataType?cap_first}${operator.name}IntColumnTransformer(returnType, left, right);
      case INT64:
        return new ${type.dataType?cap_first}${operator.name}LongColumnTransformer(returnType, left, right);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
    <#else>
    throw new UnsupportedOperationException("Unsupported Type");
    </#if>
  }
  <#else>

  @Override
  public ColumnTransformer visit${newType}(ColumnTransformer left, ColumnTransformer right) {
    Type returnType = ${operator.name}Resolver.checkConditions(left.getType(), right.getType());
    switch (right.getType().getTypeEnum()) {
      case INT32:
      <#if operator.name == "Addition" && type.type == "IntType" || type.type == "LongType" >
      case DATE:
      </#if>
        return new ${type.dataType?cap_first}${operator.name}IntColumnTransformer(returnType, left, right);
      case INT64:
      <#if operator.name == "Addition" && type.type == "IntType" || type.type == "LongType" >
      case TIMESTAMP:
      </#if>
        return new ${type.dataType?cap_first}${operator.name}LongColumnTransformer(returnType, left, right);
      case FLOAT:
        return new ${type.dataType?cap_first}${operator.name}FloatColumnTransformer(returnType, left, right);
      case DOUBLE:
        return new ${type.dataType?cap_first}${operator.name}DoubleColumnTransformer(returnType, left, right);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
  </#if>
  </#list>

}
</#list>