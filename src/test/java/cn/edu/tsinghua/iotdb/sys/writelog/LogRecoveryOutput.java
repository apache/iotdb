package cn.edu.tsinghua.iotdb.sys.writelog;


import cn.edu.tsinghua.iotdb.sys.writelog.impl.LocalFileLogReader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * This class is used to recover the data in system log file.
 */
public class LogRecoveryOutput {

    public static void main(String[] args) throws IOException {
        PrintStream ps = new PrintStream(new FileOutputStream("src/test/resources/out.txt"));
        System.setOut(ps);

        LocalFileLogReader reader = new LocalFileLogReader("/Users/beyyes/root.performf.group_0.log");
        reader.outputAllValidLog();

    }
}
