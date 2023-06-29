package org.apache.iotdb.flink.sql.wrapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SchemaWrapper implements Serializable {
  private List<Tuple2<String, DataType>> schema;

  public SchemaWrapper(TableSchema schema) {
    this.schema = new ArrayList<>();

    for (String fieldName : schema.getFieldNames()) {
      if (fieldName == "Time_") {
        continue;
      }
      this.schema.add(new Tuple2<>(fieldName, schema.getFieldDataType(fieldName).get()));
    }
  }

  public List<Tuple2<String, DataType>> getSchema() {
    return schema;
  }
}
