package org.apache.iotdb.db.pipe.connector.cdc;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.commons.lang.NotImplementedException;

import java.net.InetSocketAddress;

public class IoTDBCDCConnector implements PipeConnector {
  private final String CDC_PORT = "cdc.port";
  private int cdcPort = 8080;
  private IoTDBWebSocketServer server;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    cdcPort = parameters.getInt(CDC_PORT);
    server = new IoTDBWebSocketServer(new InetSocketAddress(cdcPort));
    server.start();
  }

  @Override
  public void handshake() throws Exception {}

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    Tablet tablet = null;
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      tablet = ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
    } else {
      throw new NotImplementedException(
          "IoTDBCDCConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent.");
    }
    server.broadcast(tablet.serialize());
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {}

  @Override
  public void transfer(Event event) throws Exception {}

  @Override
  public void close() throws Exception {
    server.stop();
  }
}
