package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TestTsFileOutput implements TsFileOutput {

  PublicBAOS publicBAOS = new PublicBAOS();

  @Override
  public void write(byte[] b) throws IOException {
    publicBAOS.write(b);
  }

  @Override
  public void write(byte b) {
    publicBAOS.write(b);
  }

  @Override
  public void write(ByteBuffer b) {
    publicBAOS.write(b.array(), b.position(), b.limit());
  }

  @Override
  public long getPosition() {
    return publicBAOS.size();
  }

  @Override
  public void close() throws IOException {
    publicBAOS.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return publicBAOS;
  }

  @Override
  public void flush() throws IOException {
    publicBAOS.flush();
  }

  @Override
  public void truncate(long size) {
    publicBAOS.truncate((int) size);
  }
}
