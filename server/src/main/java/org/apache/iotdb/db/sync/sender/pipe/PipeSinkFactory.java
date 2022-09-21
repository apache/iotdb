package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;

// TODO(Ext-pipe): move to subclass of PipeSink
public class PipeSinkFactory {
  public static PipeSink createPipeSink(String type, String name) {
    type = type.toLowerCase();

    if (PipeSink.PipeSinkType.IoTDB.name().toLowerCase().equals(type)) {
      return new IoTDBPipeSink(name);
    }

    if (ExtPipePluginRegister.getInstance().pluginExist(type)) {
      return new ExternalPipeSink(name, type);
    }

    throw new UnsupportedOperationException(
        String.format("Do not support pipeSink type: %s.", type));
  }
}
