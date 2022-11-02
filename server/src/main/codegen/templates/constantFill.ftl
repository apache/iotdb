<@pp.dropOutputFile />

<#list allDataTypes.types as type>

  <#assign className = "${type.dataType?cap_first}ConstantFill">
  <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/process/fill/constant/${className}.java" />
package org.apache.iotdb.db.mpp.execution.operator.process.fill.constant;

import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.tsfile.read.common.block.column.${type.column};
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
<#if type.dataType == "Binary">
import org.apache.iotdb.tsfile.utils.Binary;
</#if>

import java.util.Optional;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
public class ${className} implements IFill {

  // fill value
  private final ${type.dataType} value;
  // used for constructing RunLengthEncodedColumn, size of it must be 1
  private final ${type.dataType}[] valueArray;

  public ${className}(${type.dataType} value) {
    this.value = value;
    this.valueArray = new ${type.dataType}[] {value};
  }

  @Override
  public Column fill(Column valueColumn) {
    int size = valueColumn.getPositionCount();
    // if this valueColumn doesn't have any null value, or it's empty, just return itself;
    if (!valueColumn.mayHaveNull() || size == 0) {
      return valueColumn;
    }
    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      return new RunLengthEncodedColumn(new ${type.column}(1, Optional.empty(), valueArray), size);
    } else {
      ${type.dataType}[] array = new ${type.dataType}[size];
      for (int i = 0; i < size; i++) {
        if (valueColumn.isNull(i)) {
          array[i] = value;
        } else {
          array[i] = valueColumn.get${type.dataType?cap_first}(i);
        }
      }
      return new ${type.column}(size, Optional.empty(), array);
    }
  }
}

</#list>