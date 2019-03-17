package cn.edu.tsinghua.tsfile;

import org.apache.iotdb.tsfile.io.HDFSInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class tmp {
    public static void main(String[] args)throws IOException {
//        HDFSInput in = new HDFSInput("hdfs://localhost:9000/usr/hadoop/test2.tsfile/part-m-00000");
        HDFSInput in = new HDFSInput("file:/home/rl/github/incubator-iotdb/spark/src/test/resources/tsfile/test2.tsfile");
        int size = 2000;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        int len = in.read(buffer, 12);
        System.out.println("len "+len);

    }
}
