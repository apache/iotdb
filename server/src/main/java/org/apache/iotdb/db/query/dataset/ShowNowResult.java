package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ShowNowResult extends ShowResult{
    private String ip;
    private String systemTime;
    private String cpuLoad;
    private String totalMemroySize;
    private String freeMemroySize;

    public ShowNowResult(String ip, String systemTime, String cpuLoad, String totalMemroySize, String freeMemroySize) {
        this.ip = ip;
        this.systemTime = systemTime;
        this.cpuLoad = cpuLoad;
        this.totalMemroySize = totalMemroySize;
        this.freeMemroySize = freeMemroySize;
    }

    public ShowNowResult() {
        super();
    }

    public String getIp() {
        return ip;
    }

    public String getSystemTime() {
        return systemTime;
    }

    public String getCpuLoad() {
        return cpuLoad;
    }

    public String getTotalMemroySize() {
        return totalMemroySize;
    }

    public String getFreeMemroySize() {
        return freeMemroySize;
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

    public void setTotalMemroySize(String totalMemroySize) {
        this.totalMemroySize = totalMemroySize;
    }

    public void setFreeMemroySize(String freeMemroySize) {
        this.freeMemroySize = freeMemroySize;
    }

    public void serialize(OutputStream outputStream) throws IOException {
        ReadWriteIOUtils.write(ip, outputStream);
        ReadWriteIOUtils.write(systemTime, outputStream);
        ReadWriteIOUtils.write(cpuLoad, outputStream);
        ReadWriteIOUtils.write(totalMemroySize, outputStream);
        ReadWriteIOUtils.write(freeMemroySize, outputStream);
    }

    public static ShowNowResult deserialize(ByteBuffer buffer) {
        ShowNowResult result = new ShowNowResult();

        result.ip = ReadWriteIOUtils.readString(buffer);
        result.systemTime = ReadWriteIOUtils.readString(buffer);
        result.cpuLoad = ReadWriteIOUtils.readString(buffer);
        result.totalMemroySize = ReadWriteIOUtils.readString(buffer);
        result.freeMemroySize = ReadWriteIOUtils.readString(buffer);
        return result;
    }

    @Override
    public String toString() {
        return "ShowNowResult{" +
                "ip='" + ip + '\'' +
                ", systemTime='" + systemTime + '\'' +
                ", cpuLoad='" + cpuLoad + '\'' +
                ", totalMemroySize='" + totalMemroySize + '\'' +
                ", freeMemroySize='" + freeMemroySize + '\'' +
                '}';
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
                && Objects.equals(systemTime, that.systemTime)
                && Objects.equals(cpuLoad, that.cpuLoad)
                && Objects.equals(totalMemroySize, that.totalMemroySize)
                && Objects.equals(freeMemroySize, that.freeMemroySize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, systemTime, cpuLoad, totalMemroySize, freeMemroySize);
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
