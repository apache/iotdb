package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.SWAB.swab_framework;

public class Test7 {
    // 用于验证java swab实现正确性
    public static void main(String[] args) {
        Random rand = new Random(10);
        String input = "D:\\datasets\\regular\\tmp2.csv";
        boolean hasHeader = false;
        int timeIdx = 0;
        int valueIdx = 1;
        int N = 10000;
        Polyline polyline = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
//        Polyline polyline = new Polyline();
//        for (int i = 0; i < N; i += 1) {
//            double v = rand.nextInt(1000);
//            polyline.addVertex(new Point(i, v));
//        }
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

//        long startTime = System.currentTimeMillis();
//        Object[] prefixSum = prefixSum(polyline.getVertices());
//        long endTime = System.currentTimeMillis();
//        System.out.println("Time taken to precompute prefix sum: " + (endTime - startTime) + "ms");

        double maxError = 0.2;
        int m = 1000;

        // 33s worst-case O(n2)因为没有prefix sum加速计算L2误差，但是实际达不到worst-case所以实际运行不会那么大耗时
        long startTime = System.currentTimeMillis();
        List<Point> sampled = swab_framework(polyline.getVertices(), maxError, m, null, true);
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");

//        for (Point p : sampled) {
//            System.out.println(p);
//        }
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
