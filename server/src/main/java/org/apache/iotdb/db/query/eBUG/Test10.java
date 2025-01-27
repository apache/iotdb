package org.apache.iotdb.db.query.eBUG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.DP.dynamic_programming;

public class Test10 {
    // 用于验证java DP正确性
    public static void main(String[] args) {
        Random rand = new Random(10);
        String input = "raw.csv";
        boolean hasHeader = true;
        int timeIdx = 0;
        int valueIdx = 1;
        int N = 100;
//        List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
        Polyline polyline = new Polyline();
        for (int i = 0; i < N; i += 1) {
            double v = rand.nextInt(1000);
            polyline.addVertex(new Point(i, v));
        }
        List<Point> points = polyline.getVertices();
        try (FileWriter writer = new FileWriter("raw.csv")) {
            // 写入CSV头部
            writer.append("x,y,z\n");

            // 写入每个点的数据
            for (Point point : points) {
                writer.append(point.x + "," + point.y + "," + point.z + "\n");
            }
            System.out.println(points.size() + " Data has been written");
        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }


        long startTime = System.currentTimeMillis();
//        double[][] ad2 = prepareKSegments(points, ERROR.L1, false);
        int m = 10;
        List<Point> sampled = dynamic_programming(points, m - 1, DP.ERROR.area, false);
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");

        for (Point p : sampled) {
            System.out.println(p);
        }
        System.out.println(sampled.size());

//        try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
//            // 写入字符串
//            for (int i = 0; i < sampled.size(); i++) {
//                writer.println(sampled.get(i).x + "," + sampled.get(i).y);
//            }
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
    }
}
