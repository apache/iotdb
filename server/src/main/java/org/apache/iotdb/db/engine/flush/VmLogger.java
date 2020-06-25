package org.apache.iotdb.db.engine.flush;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

public class VmLogger {

  public static final String VM_LOG_NAME = ".vm.log";

  static final String FORMAT_DEVICE_OFFSET = "%s %s";

  private BufferedWriter logStream;

  public VmLogger(String storageGroupDir, String tsfileName) throws IOException {
    logStream = new BufferedWriter(
        new FileWriter(SystemFileFactory.INSTANCE.getFile(storageGroupDir,
            tsfileName + VM_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logDevice(String device, long offset) throws IOException {
    logStream.write(String.format(FORMAT_DEVICE_OFFSET, device, offset));
    logStream.newLine();
    logStream.flush();
  }

}
