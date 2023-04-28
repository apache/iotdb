package org.apache.iotdb.db.metadata.view.viewExpression.multi;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class FunctionViewExpression extends ViewExpression {

  // region member variables and init functions
  private final String functionName;

  /**
   * for a map {key1: value1, key2: value2}, this struct saves String[]{"key1", "value1", "key2",
   * "value2"}
   */
  private final List<String> functionAttributesKeyValueList;

  /**
   * example: select udf(a, b, udf(c)) from root.sg.d;
   *
   * <p>3 expressions [root.sg.d.a, root.sg.d.b, udf(root.sg.d.c)] will be in this field.
   */
  private List<ViewExpression> expressions;

  public FunctionViewExpression(String functionName) {
    this.functionName = functionName;
    functionAttributesKeyValueList = new ArrayList<>();
    expressions = new ArrayList<>();
  }

  public FunctionViewExpression(
      String functionName,
      List<String> functionAttributeKeys,
      List<String> functionAttributeValues,
      List<ViewExpression> expressions) {
    this.functionName = functionName;
    this.functionAttributesKeyValueList = new ArrayList<>();
    if (functionAttributeKeys.size() == functionAttributeValues.size()) {
      for (int i = 0; i < functionAttributeKeys.size(); i++) {
        this.functionAttributesKeyValueList.add(functionAttributeKeys.get(i));
        this.functionAttributesKeyValueList.add(functionAttributeValues.get(i));
      }
    } else {
      String errorMsg =
          String.format(
              "Illegal parameters during FunctionExpression construction. Array length mismatch.");
      throw new RuntimeException(errorMsg);
    }
    this.expressions = expressions;
  }

  public FunctionViewExpression(ByteBuffer byteBuffer) {
    functionName = ReadWriteIOUtils.readString(byteBuffer);
    functionAttributesKeyValueList = ReadWriteIOUtils.readStringList(byteBuffer);
    int expressionSize = ReadWriteIOUtils.readInt(byteBuffer);
    expressions = new ArrayList<>();
    for (int i = 0; i < expressionSize; i++) {
      expressions.add(ViewExpression.deserialize(byteBuffer));
    }
  }

  public FunctionViewExpression(InputStream inputStream) {
    try {
      functionName = ReadWriteIOUtils.readString(inputStream);
      functionAttributesKeyValueList = ReadWriteIOUtils.readStringList(inputStream);
      int expressionSize = ReadWriteIOUtils.readInt(inputStream);
      expressions = new ArrayList<>();
      for (int i = 0; i < expressionSize; i++) {
        expressions.add(ViewExpression.deserialize(inputStream));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitFunctionExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.FUNCTION;
  }

  @Override
  protected boolean isLeafOperandInternal() {
    // if this expression has no children, return true; else return false.
    if (this.expressions.size() == 0) {
      return true;
    }
    return false;
  }

  @Override
  public List<ViewExpression> getChildViewExpressions() {
    return this.expressions;
  }

  @Override
  public String toString(boolean isRoot) {
    StringBuilder result = new StringBuilder(this.functionName);
    int keyValueSize = this.functionAttributesKeyValueList.size();
    if (this.functionAttributesKeyValueList.size() > 1) {
      result.append("(");
      for (int i = 0; i + 1 < keyValueSize; i += 2) {
        result
            .append(this.functionAttributesKeyValueList.get(i))
            .append("=")
            .append(this.functionAttributesKeyValueList.get(i + 1));
        if (i + 2 >= keyValueSize) {
          break;
        }
        result.append(", ");
      }
      result.append(")");
    }
    result.append("(");
    for (int i = 0; i < this.expressions.size(); i++) {
      result.append(this.expressions.get(i).toString());
      if (i + 1 >= this.expressions.size()) {
        break;
      }
      result.append(", ");
    }
    result.append(")");
    return result.toString();
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(functionName, byteBuffer);
    ReadWriteIOUtils.writeStringList(functionAttributesKeyValueList, byteBuffer);
    ReadWriteIOUtils.write(expressions.size(), byteBuffer);
    for (ViewExpression expression : expressions) {
      ViewExpression.serialize(expression, byteBuffer);
    }
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(functionName, stream);
    ReadWriteIOUtils.writeStringList(functionAttributesKeyValueList, stream);
    ReadWriteIOUtils.write(expressions.size(), stream);
    for (ViewExpression expression : expressions) {
      ViewExpression.serialize(expression, stream);
    }
  }
  // endregion

  public String getFunctionName() {
    return this.functionName;
  }

  public LinkedHashMap<String, String> getFunctionAttributes() {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i + 1 < this.functionAttributesKeyValueList.size(); i += 2) {
      result.put(
          this.functionAttributesKeyValueList.get(i),
          this.functionAttributesKeyValueList.get(i + 1));
    }
    return result;
  }

  public List<ViewExpression> getExpressions() {
    return this.expressions;
  }
}
