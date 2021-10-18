package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ShowNowResult extends ShowResult {
  private String ip;
  private String systemTime;
  private String cpuLoad;
  private String totalMemorySize;
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
    this.totalMemorySize = totalMemoerySize;
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
    return totalMemorySize;
  }

  public String getFreeMemorySize() {
    return freeMemorySize;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(ip, outputStream);
    ReadWriteIOUtils.write(systemTime, outputStream);
    ReadWriteIOUtils.write(cpuLoad, outputStream);
    ReadWriteIOUtils.write(totalMemorySize, outputStream);
    ReadWriteIOUtils.write(freeMemorySize, outputStream);
  }

  public static ShowNowResult deserialize(ByteBuffer buffer) {
    ShowNowResult result = new ShowNowResult();

    result.ip = ReadWriteIOUtils.readString(buffer);
    result.systemTime = ReadWriteIOUtils.readString(buffer);
    result.cpuLoad = ReadWriteIOUtils.readString(buffer);
    result.totalMemorySize = ReadWriteIOUtils.readString(buffer);
    result.freeMemorySize = ReadWriteIOUtils.readString(buffer);
    return result;
  }

  @Override
  public String toString() {
    return "ShowNowResult{"
        + "ip='"
        + ip
        + '\''
        + ", systemTime='"
        + systemTime
        + '\''
        + ", cpuLoad='"
        + cpuLoad
        + '\''
        + ", totalMemoerySize='"
        + totalMemorySize
        + '\''
        + ", freeMemorySize='"
        + freeMemorySize
        + '\''
        + '}';
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public void setSystemTime(String systemTime) {
    this.systemTime = systemTime;
  }

  public void setCpuLoad(String cpuLoad) {
    this.cpuLoad = cpuLoad;
  }

  public void setTotalMemoerySize(String totalMemoerySize) {
    this.totalMemorySize = totalMemoerySize;
  }

  public void setFreeMemorySize(String freeMemorySize) {
    this.freeMemorySize = freeMemorySize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, systemTime, cpuLoad, totalMemorySize, freeMemorySize);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowNowResult that = (ShowNowResult) o;

    return Objects.equals(ip, that.ip)
        && systemTime.equals(that.systemTime)
        && cpuLoad.equals(that.cpuLoad)
        && totalMemorySize.equals(that.totalMemorySize)
        && freeMemorySize.equals(that.freeMemorySize);
  }

  @Override
  public int compareTo(ShowResult o) {
    ShowNowResult result = (ShowNowResult) o;
    if (ip.compareTo(result.getIp()) != 0) {
      return ip.compareTo(result.getIp());
    }
    return 0;
  }
}
