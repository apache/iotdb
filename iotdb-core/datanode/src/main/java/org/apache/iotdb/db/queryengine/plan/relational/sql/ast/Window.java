package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Window {
  void serialize(DataOutputStream stream) throws IOException;
}
