package org.apache.iotdb.db.metadata.visitor;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.DataTypeFilter;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;

public class TimeseriesFilterVisitor extends SchemaFilterVisitor<Boolean, ITimeSeriesSchemaInfo> {
  @Override
  public Boolean visitNode(SchemaFilter filter, ITimeSeriesSchemaInfo info) {
    return true;
  }

  @Override
  public Boolean visitPathContainsFilter(
      PathContainsFilter pathContainsFilter, ITimeSeriesSchemaInfo info) {
    if (pathContainsFilter.getContainString() == null) {
      return true;
    }
    return info.getFullPath().toLowerCase().contains(pathContainsFilter.getContainString());
  }

  @Override
  public Boolean visitDataTypeFilter(DataTypeFilter dataTypeFilter, ITimeSeriesSchemaInfo info) {
    return info.getSchema().getType() == dataTypeFilter.getDataType();
  }
}
