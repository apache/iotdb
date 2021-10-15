package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ShowNowResult extends ShowResult {
  private String ip;
  private String systemTime;
  private String cpuLoad;
  private String totalMemoerySize;
  private String freeMemorySize;

  public ShowNowResult(
      String ip,
      String systemTime,
      String cpuLoad,
      String totalMemoerySize,
      String freeMemorySize) {
    this.ip = ip;
    this.systemTime = systemTime;
    this.cpuLoad = cpuLoad;
    this.totalMemoerySize = totalMemoerySize;
    this.freeMemorySize = freeMemorySize;
  }

  public ShowNowResult() {
    super();
  }

  public String getCpuLoad() {
    return cpuLoad;
  }

  public String getIp() {
    return ip;
  }

  public String getSystemTime() {
    return systemTime;
  }

  public String getTotalMemoerySize() {
    return totalMemoerySize;
  }

  public String getFreeMemorySize() {
    return freeMemorySize;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(ip, outputStream);
    ReadWriteIOUtils.write(systemTime, outputStream);
    ReadWriteIOUtils.write(cpuLoad, outputStream);
    ReadWriteIOUtils.write(totalMemoerySize, outputStream);
    ReadWriteIOUtils.write(freeMemorySize, outputStream);
  }

  public static ShowNowResult deserialize(ByteBuffer buffer) {
    ShowNowResult result = new ShowNowResult();

    result.ip = ReadWriteIOUtils.readString(buffer);
    result.systemTime = ReadWriteIOUtils.readString(buffer);
    result.cpuLoad = ReadWriteIOUtils.readString(buffer);
    result.totalMemoerySize = ReadWriteIOUtils.readString(buffer);
    result.freeMemorySize = ReadWriteIOUtils.readString(buffer);
    return result;
  }
}
