package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AlterColumnDataType extends Statement {
  private final QualifiedName tableName;
  private final Identifier columnName;
  private final DataType dataType;
  private final boolean ifTableExists;
  private final boolean ifColumnExists;

  public AlterColumnDataType(
      @Nullable NodeLocation location,
      QualifiedName tableName,
      Identifier columnName,
      DataType dataType,
      boolean ifTableExists,
      boolean ifColumnExists) {
    super(location);
    this.tableName = tableName;
    this.columnName = columnName;
    this.dataType = dataType;
    this.ifTableExists = ifTableExists;
    this.ifColumnExists = ifColumnExists;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterColumnDataType that = (AlterColumnDataType) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(dataType, that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnName, dataType);
  }

  @Override
  public String toString() {
    return "AlterColumnDataType{"
        + "tableName="
        + tableName
        + ", columnName="
        + columnName
        + ", dataType="
        + dataType
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterColumnDataType(this, context);
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public Identifier getColumnName() {
    return columnName;
  }

  public DataType getDataType() {
    return dataType;
  }

  public boolean isIfTableExists() {
    return ifTableExists;
  }

  public boolean isIfColumnExists() {
    return ifColumnExists;
  }
}
