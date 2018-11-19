package cn.edu.tsinghua.tsfile.timeseries.basis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import org.json.JSONObject;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author Jinrui Zhang
 */
public class TsFile {

  private static final int WRITE = 0;
  private static final int READ = 1;
  private QueryEngine queryEngine;
  private int status;
  private TsFileWriter writer;
  private FileSchema fileSchema;

  /**
   * For Write
   *
   * @param file
   *          a TsFile
   * @param schemaJson
   *          the fileSchema of TsFile in type of JSON
   * @throws IOException
   *           exception in IO
   * @throws WriteProcessException
   *           exception in write process
   */
  public TsFile(File file, JSONObject schemaJson) throws IOException, WriteProcessException {
    this(file, new FileSchema(schemaJson));
  }

  /**
   * For Write
   *
   * @param file
   *          a TsFile
   * @param schema
   *          the fileSchema of TsFile
   * @throws IOException
   *           cannot write TsFile
   * @throws WriteProcessException
   *           error occurs when writing
   */
  public TsFile(File file, FileSchema schema) throws IOException, WriteProcessException {
    this(schema);
    writer = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * For Write
   *
   * @param output
   *          a TsFile
   * @param schemaJson
   *          the fileSchema of TsFile in type of JSON
   * @throws IOException
   *           exception in IO
   * @throws WriteProcessException
   *           exception in write process
   */
  public TsFile(ITsRandomAccessFileWriter output, JSONObject schemaJson)
      throws IOException, WriteProcessException {
    this(new FileSchema(schemaJson));
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    writer = new TsFileWriter(output, fileSchema, conf);
  }

  /**
   * For Write
   *
   * @param output
   *          a TsFile
   * @param schema
   *          the fileSchema of TsFile
   * @throws IOException
   *           cannot write TsFile
   * @throws WriteProcessException
   *           error occurs when writing
   */
  public TsFile(ITsRandomAccessFileWriter output, FileSchema schema)
      throws IOException, WriteProcessException {
    this(schema);
    writer = new TsFileWriter(output, fileSchema, TSFileDescriptor.getInstance().getConfig());
  }

  private TsFile(FileSchema schema) {
    fileSchema = schema;
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    if (fileSchema.hasProp(JsonFormatConstant.ROW_GROUP_SIZE))
      conf.groupSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.ROW_GROUP_SIZE));
    if (fileSchema.hasProp(JsonFormatConstant.PAGE_SIZE))
      conf.pageSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.PAGE_SIZE));
    this.status = WRITE;
  }

  /**
   * Notice: This constructor is only for reading TsFile.
   *
   * @param raf
   *          input reader
   * @throws IOException
   *           cannot read TsFile
   */
  public TsFile(ITsRandomAccessFileReader raf) throws IOException {
    this.status = READ;
    queryEngine = new QueryEngine(raf);
    // recordReader = queryEngine.recordReader;
  }

  /**
   * write a line into TsFile <br>
   * the corresponding schema must be defined.
   * 
   * @param line
   *          a line of data
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   * @throws WriteProcessException
   *           thrown if given data is not matched to fileSchema
   */
  public void writeLine(String line) throws IOException, WriteProcessException {
    checkStatus(WRITE);
    TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
    writer.write(record);
  }

  /**
   * add a new property, replace old value if already exist.
   *
   * @param key
   *          key of property
   * @param value
   *          value of property
   */
  public void addProp(String key, String value) {
    fileSchema.addProp(key, value);
  }

  /**
   * write a TSRecord into TsFile.
   *
   * @param tsRecord
   *          a line of data in form of {@linkplain TSRecord}
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   * @throws WriteProcessException
   *           thrown if given data is not matched to fileSchema
   */
  public void writeRecord(TSRecord tsRecord) throws IOException, WriteProcessException {
    checkStatus(WRITE);
    writer.write(tsRecord);
  }

  /**
   * end the write process normally.
   *
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   */
  public void close() throws IOException {
    if (this.status == WRITE) {
      writer.close();
    } else if (this.status == READ) {
      queryEngine.close();
    } else {
      String[] msg = new String[] { "WRITE", "READ" };
      throw new IOException("This method should be invoked in status " + msg[status]
          + ", but current status is " + msg[this.status]);
    }
  }


  public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                                   FilterExpression valueFilter) throws IOException {
    checkStatus(READ);
    if (paths.size() == 1 && valueFilter instanceof SingleSeriesFilterExpression
        && paths.get(0).getDeltaObjectToString()
            .equals(valueFilter.getFilterSeries().getDeltaObjectUID())
        && paths.get(0).getMeasurementToString()
            .equals(valueFilter.getFilterSeries().getMeasurementUID())) {

    } else if (valueFilter != null) {
      valueFilter = FilterFactory.csAnd(valueFilter, valueFilter);
    }
    return queryEngine.query(paths, timeFilter, null, valueFilter);
  }


  public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                                   FilterExpression valueFilter, Map<String, Long> params) throws IOException {
    checkStatus(READ);
    return queryEngine.query(paths, timeFilter, null, valueFilter, params);
  }

  /**
   * Get All information of column(s) for every deltaObject.
   *
   * @return A set of ArrayList SeriesSchema stored in a HashMap separated by deltaObjectId
   * @throws IOException
   *           thrown if fail to get all series schema
   */
  public Map<String, ArrayList<SeriesSchema>> getAllColumns() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllSeriesSchemasGroupByDeltaObject();
  }

  /**
   * Get RowGroupSize for every deltaObject
   *
   * @return HashMap
   * @throws IOException
   *           thrown if fail to get row group count
   */
  public Map<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
    checkStatus(READ);
    return queryEngine.getDeltaObjectRowGroupCount();
  }

  /**
   * @return a map contains all DeltaObjects with type each.
   * @throws IOException
   *           thrown if fail to get delta object type
   */
  public Map<String, String> getDeltaObjectTypes() throws IOException {
    checkStatus(READ);
    return queryEngine.getDeltaObjectTypes();
  }

  /**
   * Check whether given path exists in this TsFile.
   *
   * @param path
   *          A path of one Series
   * @return if the path exists
   * @throws IOException
   *           thrown if fail to check path exists
   */
  public boolean pathExist(Path path) throws IOException {
    checkStatus(READ);
    return queryEngine.pathExist(path);
  }

  /**
   * @return all deltaObjects' name in current TsFile
   * @throws IOException
   *           thrown if fail to get all delta object
   */
  public ArrayList<String> getAllDeltaObject() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllDeltaObject();
  }

  /**
   * @return all series' schemas in current TsFile
   * @throws IOException
   *           thrown if fail to all series
   */
  public List<SeriesSchema> getAllSeries() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllSeriesSchema();
  }

  /**
   * Get all RowGroups' offsets in current TsFile
   *
   * @return res.get(i) represents the End-Position for specific rowGroup i in this file.
   * @throws IOException
   *           thrown if fail to get row group pos list
   */
  public ArrayList<Long> getRowGroupPosList() throws IOException {
    checkStatus(READ);
    return queryEngine.getRowGroupPosList();
  }

  public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end)
      throws IOException {
    checkStatus(READ);
    return queryEngine.calSpecificRowGroupByPartition(start, end);
  }

  public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end)
      throws IOException {
    checkStatus(READ);
    return queryEngine.getAllDeltaObjectUIDByPartition(start, end);
  }

  public Map<String, String> getProps() {
    return queryEngine.getProps();
  }

  /**
   * clear and set new properties.
   *
   * @param props
   *          properties in map struct
   */
  public void setProps(Map<String, String> props) {
    fileSchema.setProps(props);
  }

  public String getProp(String key) {
    return queryEngine.getProp(key);
  }

  private void checkStatus(int status) throws IOException {
    if (status != this.status) {
      String[] msg = new String[] { "WRITE", "READ" };
      throw new IOException("This method should be invoked in status " + msg[status]
          + ", but current status is " + msg[this.status]);
    }
  }
}
