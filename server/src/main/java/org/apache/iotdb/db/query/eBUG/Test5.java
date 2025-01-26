package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.SWAB.seg_bottomUp_maxerror_withTimestamps;

public class Test5 {
    // 用于验证java bottomUpMaxError实现正确性
    public static void main(String[] args) {
        Random rand = new Random(10);
        String input = "D:\\datasets\\regular\\tmp2.csv";
        boolean hasHeader = false;
        int timeIdx = 0;
        int valueIdx = 1;
        int N = 2000;
//        Polyline polyline = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
        Polyline polyline = new Polyline();
        for (int i = 0; i < N; i += 1) {
            double v = rand.nextInt(1000);
            polyline.addVertex(new Point(i, v));
        }
        try (FileWriter writer = new FileWriter("raw.csv")) {
            // 写入CSV头部
            writer.append("x,y,z\n");

            // 写入每个点的数据
            for (int i = 0; i < polyline.size(); i++) {
                Point point = polyline.get(i);
                writer.append(point.x + "," + point.y + "," + point.z + "\n");
            }
            System.out.println(polyline.size() + " Data has been written");
        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }

        long startTime = System.currentTimeMillis();
        Object[] prefixSum = SWAB.prefixSum(polyline.getVertices());
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to precompute prefix sum: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        double maxError = 5000;
        List<Point> sampled = seg_bottomUp_maxerror_withTimestamps(polyline.getVertices(), maxError, prefixSum, false);
        endTime = System.currentTimeMillis();
        System.out.println("Time taken to : " + (endTime - startTime) + "ms");

        for (Point p : sampled) {
            System.out.println(p);
        }
        System.out.println(sampled.size());

        try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
            // 写入字符串
            for (int i = 0; i < sampled.size(); i++) {
                writer.println(sampled.get(i).x + "," + sampled.get(i).y);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
