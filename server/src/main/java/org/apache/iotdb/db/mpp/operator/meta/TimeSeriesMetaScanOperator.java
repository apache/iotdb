package org.apache.iotdb.db.mpp.operator.meta;

import java.io.IOException;
import java.util.List;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ATTRIBUTES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TAGS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class TimeSeriesMetaScanOperator extends MetaScanOperator {
  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  private boolean hasNext;

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
      boolean isPrefixPath) {
    super(operatorContext, schemaRegionId, limit, offset, partialPath, isPrefixPath);
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
    List<ShowTimeSeriesResult> showTimeSeriesResults =
        IoTDB.schemaProcessor.showTimeSeries(this, operatorContext.getInstanceContext());
    Column[] valueColums = new Column[resourcePaths.length];
    showTimeSeriesResults.forEach(series -> {

    });
    return tsBlock;
  }

  @Override
  public TsBlock next() throws IOException {

    hasNext = false;
    return tsBlock;
  }

  @Override
  public boolean hasNext() throws IOException {
    try {
      if (tsBlock == null) {
        tsBlock = createTsBlock();
        if (tsBlock != null) {
          return true;
        }
      }
      return false;
    } catch (MetadataException e) {
      throw new IOException(e);
    }
  }
}
