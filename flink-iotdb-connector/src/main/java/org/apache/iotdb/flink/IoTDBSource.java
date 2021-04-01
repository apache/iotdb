package org.apache.iotdb.flink;

import org.apache.iotdb.flink.options.IoTDBSourceOptions;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IoTDBSource<T> extends RichSourceFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(IoTDBSource.class);
  private static final long serialVersionUID = 1L;
  private IoTDBSourceOptions sourceOptions;

  private transient Session session;
  SessionDataSet dataSet;

  public IoTDBSource(IoTDBSourceOptions ioTDBSourceOptions) {
    this.sourceOptions = ioTDBSourceOptions;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    initSession();
  }

  public abstract T convert(RowRecord rowRecord);

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    dataSet = session.executeQueryStatement(sourceOptions.getSql());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      sourceContext.collect(convert(dataSet.next()));
    }
    dataSet.closeOperationHandle();
  }

  @Override
  public void cancel() {
    try {
      dataSet.closeOperationHandle();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    session.close();
  }

  void initSession() throws Exception {
    session =
        new Session(
            sourceOptions.getHost(),
            sourceOptions.getPort(),
            sourceOptions.getUser(),
            sourceOptions.getPassword());
  }
}
