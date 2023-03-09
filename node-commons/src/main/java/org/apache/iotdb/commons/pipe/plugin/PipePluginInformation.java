package org.apache.iotdb.commons.pipe.plugin;

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PipePluginInformation {

  private String pluginName;

  private String className;

  private String jarName;

  private String jarMD5;

  private PipePluginInformation() {}

  public PipePluginInformation(String pluginName, String className, String jarName, String jarMD5) {
    this.pluginName = pluginName.toUpperCase();
    this.className = className;
    this.jarName = jarName;
    this.jarMD5 = jarMD5;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getClassName() {
    return className;
  }

  public String getJarName() {
    return jarName;
  }

  public String getJarMD5() {
    return jarMD5;
  }

  public void setPluginName(String pluginName) {
    this.pluginName = pluginName.toUpperCase();
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public void setJarMD5(String jarMD5) {
    this.jarMD5 = jarMD5;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pluginName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    ReadWriteIOUtils.write(jarName, outputStream);
    ReadWriteIOUtils.write(jarMD5, outputStream);
  }

  public static PipePluginInformation deserialize(ByteBuffer byteBuffer) {
    PipePluginInformation pipePluginInformation = new PipePluginInformation();
    pipePluginInformation.pluginName = ReadWriteIOUtils.readString(byteBuffer);
    pipePluginInformation.className = ReadWriteIOUtils.readString(byteBuffer);
    pipePluginInformation.jarName = ReadWriteIOUtils.readString(byteBuffer);
    pipePluginInformation.jarMD5 = ReadWriteIOUtils.readString(byteBuffer);
    return pipePluginInformation;
  }

  public static PipePluginInformation deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }
}
