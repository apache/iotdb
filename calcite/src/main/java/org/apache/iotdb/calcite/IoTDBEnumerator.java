package org.apache.iotdb.calcite;

import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IoTDBEnumerator implements Enumerator<Object> {
  private ResultSet resultSet;
  private List<Integer> indexInResultSet = new ArrayList<>();
  private Row current;
  private List<RelDataTypeField> fieldTypes;

  /** Creates a IoTDBEnumerator.
   *
   * @param results IoTDB result set
   * @param protoRowType The type of protecting rows
   */
  IoTDBEnumerator(ResultSet results, RelProtoDataType protoRowType) throws SQLException {
    this.resultSet = results;
    this.current = null;

    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();

    // get the corresponding index of project columns in result set
    // as we have 'time' and 'device' columns in result that the project columns may don't include
    ResultSetMetaData metaData = results.getMetaData();
    for (int i = 0; i < metaData.getColumnCount(); i++){
      if (i < 2 && !metaData.getColumnName(i + 1).equals(fieldTypes.get(i).getName())){
        continue;
      } else {
        indexInResultSet.add(i + 1);
      }
    }
  }

  /** Produce and get the next row from the results
   *
   * @return A new row from the results
   */
  @Override
  public Object current() {

      // Build an array with all fields in this row
      Object[] row = new Object[fieldTypes.size()];
      for (int i = 0; i < fieldTypes.size(); i++) {
        try {
          row[i] = currentRowField(indexInResultSet.get(i), fieldTypes.get(i).getType().getSqlTypeName());
        } catch (SQLException e) {
         if(e.getMessage().endsWith("NULL.")){
           row[i] = null;
         } else {
           e.printStackTrace();
           return null;
         }
        }
      }
      return row;
  }

  /** Get a field for the current row from the underlying object.
   *
   * @param index Index of the field within the Row object
   * @param type Type of the field in this row
   */
  private Object currentRowField(int index, SqlTypeName type) throws SQLException {
    if (type == SqlTypeName.VARCHAR) {
      return resultSet.getString(index);
    } else if (type == SqlTypeName.INTEGER) {
      return resultSet.getInt(index);
    } else if (type == SqlTypeName.BIGINT) {
      return resultSet.getLong(index);
    } else if (type == SqlTypeName.DOUBLE) {
      return resultSet.getDouble(index);
    } else if (type == SqlTypeName.REAL) {
      return resultSet.getFloat(index);
    } else if (type == SqlTypeName.BOOLEAN){
      return resultSet.getBoolean(index);
    } else {
      return null;
    }
  }

  /**
   * Advances the enumerator to the next element of the collection.
   * @return whether the resultset has next element
   */
  @Override
  public boolean moveNext() {
    try {
      if (resultSet.next()) {
        return true;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Sets the enumerator to its initial position, which is before the first element in the collection.
   */
  @Override
  public void reset() { throw new UnsupportedOperationException(); }

  /**
   *  Closes this enumerable and releases resources.
   */
  @Override
  public void close() {
    try {
      resultSet.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}

// End IoTDBEnumerator.java