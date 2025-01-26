package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.SWAB.prefixSum;
import static org.apache.iotdb.db.query.eBUG.SWAB.seg_bottomUp_maxerror_withTimestamps;

public class Test6 {
    // 用于实测bottomUp-L2-with/without prefix sum acceleration和eBUG算法的耗时比较
    public static void main(String[] args) {
        Random rand = new Random(10);
        String input = "D:\\datasets\\regular\\tmp2.csv";
        boolean hasHeader = false;
        int timeIdx = 0;
        int valueIdx = 1;
        int N = 1000_0000;
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
        Object[] prefixSum = prefixSum(polyline.getVertices());
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to precompute prefix sum: " + (endTime - startTime) + "ms");

        double maxError = 500000;

        // 33s worst-case O(n2)因为没有prefix sum加速计算L2误差，但是实际达不到worst-case所以实际运行不会那么大耗时
        startTime = System.currentTimeMillis();
        List<Point> sampled = seg_bottomUp_maxerror_withTimestamps(polyline.getVertices(), maxError, null, false);
        endTime = System.currentTimeMillis();
        System.out.println("Time taken to bottomUp-L2 without prefix sum acceleration: " + (endTime - startTime) + "ms");
        System.out.println(sampled.size());

        // 30s worst case O(nlogn)主要就是heap的操作耗时了
        startTime = System.currentTimeMillis();
        sampled = seg_bottomUp_maxerror_withTimestamps(polyline.getVertices(), maxError, prefixSum, false);
        endTime = System.currentTimeMillis();
        System.out.println("Time taken to bottomUp-L2 with prefix sum acceleration:" + (endTime - startTime) + "ms");
        System.out.println(sampled.size());

        // 39s worst-case O(n2)但是实际达不到worst-case所以实际运行不会那么大耗时。
        // 不过还是会比L2误差的耗时要大，毕竟AD计算比L2更复杂一些虽然都是线性，但是常数大一些
        startTime = System.currentTimeMillis();
        sampled = eBUG.buildEffectiveArea(polyline, 1000_0000, false, 3591486);
        endTime = System.currentTimeMillis();
        System.out.println("Time taken to eBUG with e>n-3: " + (endTime - startTime) + "ms");
        System.out.println(sampled.size());

        // 33s worst case O(nlogn)主要就是heap的操作耗时了
        startTime = System.currentTimeMillis();
        sampled = eBUG.buildEffectiveArea(polyline, 0, false, 3591486);
        endTime = System.currentTimeMillis();
        System.out.println("Time taken to eBUG with e=0: " + (endTime - startTime) + "ms");
        System.out.println(sampled.size());

//        for (Point p : sampled) {
//            System.out.println(p);
//        }

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
