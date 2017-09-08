package cn.edu.tsinghua.iotdb.auth.dao;

import java.io.OutputStream;

public class DerbyUtil {
    public static final OutputStream DEV_NULL = new OutputStream() {
        public void write(int b) {}
    };
}
