package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.filter.impl.OrFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowTableDevicesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class TableDeviceSchemaSource implements ISchemaSource<IDeviceSchemaInfo> {

  private String database;

  private String tableName;

  private List<SchemaFilter> idDeterminedFilterList;

  private List<SchemaFilter> idFuzzyFilterList;

  private List<ColumnHeader> columnHeaderList;

  public TableDeviceSchemaSource(
      String database,
      String tableName,
      List<SchemaFilter> idDeterminedFilterList,
      List<SchemaFilter> idFuzzyFilterList,
      List<ColumnHeader> columnHeaderList) {
    this.database = database;
    this.tableName = tableName;
    this.idDeterminedFilterList = idDeterminedFilterList;
    this.idFuzzyFilterList = idFuzzyFilterList;
    this.columnHeaderList = columnHeaderList;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    List<PartialPath> devicePatternList = getDevicePatternList();
    return new ISchemaReader<IDeviceSchemaInfo>() {

      private ISchemaReader<IDeviceSchemaInfo> deviceReader;
      private Throwable throwable;
      private int index = 0;

      @Override
      public boolean isSuccess() {
        return throwable == null && (deviceReader == null || deviceReader.isSuccess());
      }

      @Override
      public Throwable getFailure() {
        if (throwable != null) {
          return throwable;
        } else if (deviceReader != null) {
          return deviceReader.getFailure();
        }
        return null;
      }

      @Override
      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      @Override
      public boolean hasNext() {
        try {
          if (throwable != null) {
            return false;
          }
          if (deviceReader != null) {
            if (deviceReader.hasNext()) {
              return true;
            } else {
              deviceReader.close();
              if (!deviceReader.isSuccess()) {
                throwable = deviceReader.getFailure();
                return false;
              }
            }
          }

          while (index < devicePatternList.size()) {
            deviceReader =
                schemaRegion.getTableDeviceReader(
                    new ShowTableDevicesPlan(devicePatternList.get(index), idFuzzyFilterList));
            index++;
            if (deviceReader.hasNext()) {
              return true;
            } else {
              deviceReader.close();
            }
          }
          return false;
        } catch (Exception e) {
          throw new SchemaExecutionException(e.getMessage(), e);
        }
      }

      @Override
      public IDeviceSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return deviceReader.next();
      }

      @Override
      public void close() throws Exception {
        if (deviceReader != null) {
          deviceReader.close();
        }
      }
    };
  }

  private List<PartialPath> getDevicePatternList() {
    int length = DataNodeTableCache.getInstance().getTable(database, tableName).getIdNums() + 3;
    String[] nodes = new String[length];
    Arrays.fill(nodes, "*");
    nodes[0] = PATH_ROOT;
    nodes[1] = database;
    nodes[2] = tableName;
    Map<Integer, List<String>> orValueMap = new HashMap<>();
    for (SchemaFilter schemaFilter : idDeterminedFilterList) {
      if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.DEVICE_ID)) {
        DeviceIdFilter deviceIdFilter = (DeviceIdFilter) schemaFilter;
        nodes[deviceIdFilter.getIndex() + 3] = deviceIdFilter.getValue();
      } else if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.OR)) {
        OrFilter orFilter = (OrFilter) schemaFilter;
        if (orFilter.getLeft().getSchemaFilterType().equals(SchemaFilterType.DEVICE_ID)
            && orFilter.getRight().getSchemaFilterType().equals(SchemaFilterType.DEVICE_ID)) {
          DeviceIdFilter deviceIdFilter = (DeviceIdFilter) orFilter.getLeft();
          nodes[deviceIdFilter.getIndex() + 3] = deviceIdFilter.getValue();
          deviceIdFilter = (DeviceIdFilter) orFilter.getLeft();
          orValueMap
              .computeIfAbsent(deviceIdFilter.getIndex(), k -> new ArrayList<>())
              .add(deviceIdFilter.getValue());
        }
      }
    }

    PartialPath path = new PartialPath(nodes);
    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(path);
    for (Map.Entry<Integer, List<String>> entry : orValueMap.entrySet()) {
      for (int i = 0, size = pathList.size(); i < size; i++) {
        for (String value : entry.getValue()) {
          nodes = Arrays.copyOf(pathList.get(i).getNodes(), length);
          nodes[entry.getKey() + 3] = value;
          path = new PartialPath(nodes);
          pathList.add(path);
        }
      }
    }

    return pathList;
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return columnHeaderList;
  }

  @Override
  public void transformToTsBlockColumns(
      IDeviceSchemaInfo schemaInfo, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0L);
    int resultIndex = 0;
    int idIndex = 0;
    PartialPath devicePath = schemaInfo.getPartialPath();
    TsTable table = DataNodeTableCache.getInstance().getTable(this.database, tableName);
    TsTableColumnSchema columnSchema;
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnSchema = table.getColumnSchema(columnHeader.getColumnName());
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        builder
            .getColumnBuilder(resultIndex)
            .writeBinary(
                new Binary(devicePath.getNodes()[idIndex + 3], TSFileConfig.STRING_CHARSET));
        idIndex++;
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        builder
            .getColumnBuilder(resultIndex)
            .writeBinary(
                new Binary(
                    schemaInfo.getAttributeValue(columnHeader.getColumnName()),
                    TSFileConfig.STRING_CHARSET));
      }
      resultIndex++;
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return 0;
  }
}
