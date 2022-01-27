package org.apache.iotdb;

import java.io.*;

public class ProcessResult {

    public static final String BASE_PATH = "/home/kyy/Documents/kdd";

    public static void main(String[] args) throws IOException {

        String inFilePath = BASE_PATH + File.separator + "result.txt";
        String outFilePath = BASE_PATH + File.separator + "out.txt";

        BufferedReader reader = new BufferedReader(new FileReader(inFilePath));
        FileWriter writer = new FileWriter(outFilePath);
        String readLine = null;
        long metaTime = 0;
        long dataTime = 0;
        long totalTime = 0;
        int counter = 0;
        while ((readLine = reader.readLine()) != null) {
            if (readLine.startsWith("select")) {
                String[] values = readLine.split("\t");
                metaTime += Long.parseLong(values[1].split(": ")[1]);
                dataTime += Long.parseLong(values[3].split(": ")[1]);
                totalTime += Long.parseLong(values[5].split(": ")[1]);
                counter++;
                writer.write(readLine + "\n");
            }
        }
        writer.write("avg_meta: " + (double) metaTime / counter
                + "\t avg_data: " + (double) dataTime / counter
                + "\t avg_total: " + (double) totalTime / counter);
        reader.close();
        writer.close();
    }
}
