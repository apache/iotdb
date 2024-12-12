package org.apache.iotdb.session.subscription;

import java.util.Collections;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSession;

public class SubscriptionSessionBuilder extends AbstractSessionBuilder {

  public ITableSession build() throws IoTDBConnectionException {
    if (nodeUrls == null) {
      this.nodeUrls =
          Collections.singletonList(SessionConfig.DEFAULT_HOST + ":" + SessionConfig.DEFAULT_PORT);
    }
    this.sqlDialect = "table";
    Session newSession = new Session(this);
    newSession.open(enableCompression, connectionTimeoutInMs);
    return new TableSession(newSession);
  }
}
