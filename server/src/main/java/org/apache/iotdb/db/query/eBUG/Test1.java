package org.apache.iotdb.db.query.eBUG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.eBUG.buildEffectiveArea;

public class Test1 {
    // 用于验证Java eBUG实现和python版本(e=0/1)结果的一致性
    public static void main(String[] args) {
        Polyline polyline = new Polyline();
        List<Polyline> polylineList = new ArrayList<>();
        Random rand = new Random(2);
        int n = 10000;

        int p = 10;
        for (int i = 0; i < n; i += p) {
            Polyline polylineBatch = new Polyline();
            for (int j = i; j < Math.min(i + p, n); j++) {
                double v = rand.nextInt(1000000);

                polyline.addVertex(new Point(j, v));

                polylineBatch.addVertex(new Point(j, v));
            }
            polylineList.add(polylineBatch);
        }

        try (FileWriter writer = new FileWriter("raw.csv")) {
            // 写入CSV头部
            writer.append("x,y,z\n");

            // 写入每个点的数据
            for (int i = 0; i < polyline.size(); i++) {
                Point point = polyline.get(i);
                writer.append(point.x + "," + point.y + "," + point.z + "\n");
            }
            System.out.println("Data has been written");
        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }

        System.out.println("---------------------------------");
        int eParam = 0;
        long startTime = System.currentTimeMillis();
        List<Point> results = buildEffectiveArea(polyline, eParam, false);
        // 输出结果
        long endTime = System.currentTimeMillis();
        System.out.println("n=" + n + ", e=" + eParam + ", Time taken to reduce points: " + (endTime - startTime) + "ms");
        System.out.println(results.size());

        if (results.size() <= 100) {
            System.out.println("+++++++++++++++++++");
            for (int i = 0; i < results.size(); i++) {
                Point point = results.get(i);
                System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
            }
        }

        try (FileWriter writer = new FileWriter("fast.csv")) {
            // 写入CSV头部
            writer.append("x,y,z\n");

            // 写入每个点的数据
            for (int i = 0; i < results.size(); i++) {
                Point point = results.get(i);
                writer.append(point.x + "," + point.y + "," + point.z + "\n");
            }
            System.out.println("Data has been written");
        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }
    }
}
