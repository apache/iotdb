package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;

import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class WrappedInsertStatement extends WrappedStatement {

  protected TableSchema tableSchema;

  public WrappedInsertStatement(InsertBaseStatement innerTreeStatement, MPPQueryContext context) {
    super(innerTreeStatement, context);
  }

  @Override
  public InsertBaseStatement getInnerTreeStatement() {
    return ((InsertBaseStatement) super.getInnerTreeStatement());
  }

  public abstract void updateAfterSchemaValidation(MPPQueryContext context)
      throws QueryProcessException;

  public TableSchema getTableSchema() {
    if (tableSchema == null) {
      InsertBaseStatement insertBaseStatement = getInnerTreeStatement();
      String tableName = insertBaseStatement.getDevicePath().getFullPath();
      List<ColumnSchema> columnSchemas =
          new ArrayList<>(insertBaseStatement.getMeasurements().length);
      for (int i = 0; i < insertBaseStatement.getMeasurements().length; i++) {
        columnSchemas.add(
            new ColumnSchema(
                insertBaseStatement.getMeasurements()[i],
                TypeFactory.getType(insertBaseStatement.getDataTypes()[i]),
                false,
                insertBaseStatement.getColumnCategories()[i]));
      }
      tableSchema = new TableSchema(tableName, columnSchemas);
    }

    return tableSchema;
  }
}
