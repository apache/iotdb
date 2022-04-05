package org.apache.iotdb.db.mpp.operator.meta;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesMetaScanOperator extends MetaScanOperator {
  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

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
      ConsensusGroupId schemaRegionId,
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
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(resourceTypes));
    schemaRegion
        .showTimeSeries(this, operatorContext.getInstanceContext())
        .forEach(series -> setColumns(series, builder));
    return builder.build();
  }

  private void setColumns(ShowTimeSeriesResult series, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(series.getLastTime());
    writeValueColumn(builder, 0, series.getName());
    writeValueColumn(builder, 1, series.getAlias());
    writeValueColumn(builder, 2, series.getSgName());
    writeValueColumn(builder, 3, series.getDataType().toString());
    writeValueColumn(builder, 4, series.getEncoding().toString());
    writeValueColumn(builder, 5, series.getCompressor().toString());
    writeValueColumn(builder, 6, mapToString(series.getTag()));
    writeValueColumn(builder, 7, mapToString(series.getAttribute()));
    builder.declarePosition();
  }

  private void writeValueColumn(TsBlockBuilder builder, int columnIndex, String value) {
    if (value == null) {
      builder.getColumnBuilder(columnIndex).appendNull();
    } else {
      builder.getColumnBuilder(columnIndex).writeBinary(new Binary(value));
    }
  }

  private String mapToString(Map<String, String> map) {
    return map.entrySet().stream()
        .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
        .collect(Collectors.joining(","));
  }
}
