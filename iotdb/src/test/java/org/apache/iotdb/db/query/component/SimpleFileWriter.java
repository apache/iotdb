package org.apache.iotdb.db.query.component;

import org.junit.Ignore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Ignore
public class SimpleFileWriter {

  public static void writeFile(String path, byte[] bytes) throws IOException {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    FileOutputStream fileOutputStream = new FileOutputStream(path);
    fileOutputStream.write(bytes, 0, bytes.length);
    fileOutputStream.close();
  }

  public static void writeFile(int size, String path) throws IOException {
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i % 200 + 1);
    }
    FileOutputStream fileOutputStream = new FileOutputStream(path);
    fileOutputStream.write(bytes, 0, size);
    fileOutputStream.close();
  }
}
