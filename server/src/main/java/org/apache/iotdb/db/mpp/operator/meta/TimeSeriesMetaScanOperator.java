package org.apache.iotdb.db.mpp.operator.meta;

import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ATTRIBUTES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TAGS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;

public class TimeSeriesMetaScanOperator extends MetaScanOperator {
  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  private static final Path[] resourcePaths = {
    new PartialPath(COLUMN_TIMESERIES, false),
    new PartialPath(COLUMN_TIMESERIES_ALIAS, false),
    new PartialPath(COLUMN_STORAGE_GROUP, false),
    new PartialPath(COLUMN_TIMESERIES_DATATYPE, false),
    new PartialPath(COLUMN_TIMESERIES_ENCODING, false),
    new PartialPath(COLUMN_TIMESERIES_COMPRESSION, false),
    new PartialPath(COLUMN_TAGS, false),
    new PartialPath(COLUMN_ATTRIBUTES, false)
  };
  private static final TSDataType[] resourceTypes = {
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT
  };

  public TimeSeriesMetaScanOperator(
      OperatorContext operatorContext,
      SchemaRegionId schemaRegionId,
      int limit,
      int offset,
      PartialPath partialPath,
      String key,
      String value,
      boolean isContains,
      boolean orderByHeat,
      boolean isPrefixPath,
      List<String> columns) {
    super(operatorContext, schemaRegionId, limit, offset, partialPath, isPrefixPath, columns);
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.orderByHeat = orderByHeat;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public boolean isContains() {
    return isContains;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  @Override
  protected TsBlock createTsBlock() throws MetadataException {
    if (isOrderByHeat() && limit != 0) {
      limit += offset;
      offset = 0;
    }
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(resourceTypes));
    schemaRegion
        .showTimeSeries(this, operatorContext.getInstanceContext())
        .forEach(
            series -> {
              builder.getTimeColumnBuilder().writeLong(series.getLastTime());
              builder.getColumnBuilder(0).writeBinary(new Binary(series.getName()));
              builder.getColumnBuilder(1).writeBinary(new Binary(series.getAlias()));
              builder.getColumnBuilder(2).writeBinary(new Binary(series.getSgName()));
              builder.getColumnBuilder(3).writeBinary(new Binary(series.getDataType().toString()));
              builder.getColumnBuilder(4).writeBinary(new Binary(series.getEncoding().toString()));
              builder
                  .getColumnBuilder(5)
                  .writeBinary(new Binary(series.getCompressor().toString()));
              builder.getColumnBuilder(6).writeBinary(new Binary(mapToString(series.getTag())));
              builder
                  .getColumnBuilder(7)
                  .writeBinary(new Binary(mapToString(series.getAttribute())));
              builder.declarePosition();
            });
    return builder.build();
  }

  private String mapToString(Map<String, String> map) {
    return map.entrySet().stream()
        .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
        .collect(Collectors.joining(","));
  }
}
