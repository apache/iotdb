package org.apache.iotdb.db.query.eBUG;

import java.util.*;

public class TimestampPriorityQueue {
    private final int e;  // 队列的最大大小
    private final LinkedList<Point> insertionOrder;  // 保持插入顺序
    public final PriorityQueue<Point> queue;  // 按时间戳排序的优先队列

    // 构造函数：初始化队列大小、插入顺序队列和优先队列
    public TimestampPriorityQueue(int e) {
        this.e = e;
        this.insertionOrder = new LinkedList<>();
        this.queue = new PriorityQueue<>(Comparator.comparingDouble(p -> p.x));
    }

    public int size() {
        return queue.size();
    }

    // 插入新的元素
    public void insert(Point newPoint) {
        // 如果队列已满，移除最早插入的点
        if (queue.size() == e) {
            Point removedPoint = insertionOrder.removeFirst();  // 移除最早加入的点 复杂度1
            queue.remove(removedPoint);  // 从优先队列中移除该点 TODO 复杂度O(e)!
        }

        // 将新点添加到插入顺序队列中
        insertionOrder.addLast(newPoint); // 复杂度1

        // 插入到优先队列，保持时间戳顺序
        queue.offer(newPoint); // 复杂度log(e)
    }

    // 获取队列中的所有元素（按时间戳排序）
    public List<Point> getQueue() {
        return new ArrayList<>(queue); // 浅复制
    }

    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        for (Point point : queue) {
            stringBuffer.append(point.toString());
            stringBuffer.append(",");
        }
        return stringBuffer.toString();
    }

    public static void main(String[] args) {
        TimestampPriorityQueue tq = new TimestampPriorityQueue(3);

        tq.insert(new Point(10, 1000));
        tq.insert(new Point(20, 2000));
        tq.insert(new Point(15, 1500));
        tq.insert(new Point(25, 500));

        // 打印队列内容
        for (Point point : tq.getQueue()) {
            System.out.println(point);
        }
    }
}


//public class TimestampPriorityQueue { // for managing the e most recently eliminated points, sorted by time in ascending order
//    private final int e;
//    public final PriorityQueue<Point> queue;
//
//    // 构造函数：初始化优先队列的最大大小
//    public TimestampPriorityQueue(int e) {
//        this.e = e;
//        this.queue = new PriorityQueue<>(Comparator.comparingDouble(p -> p.x));
//    }
//
//    // 插入新的元素
//    public void insert(Point newPoint) {
//        // 如果队列已满，移除最小（最早）的元素
//        if (queue.size() == e) {
//            queue.poll();
//        }
//
//        // 插入元素，队列会根据时间戳自动排序
//        queue.offer(newPoint);
//    }
//
//    // 获取队列中的所有元素
//    public List<Point> getQueue() {
//        return new ArrayList<>(queue);
//    }
//
//    public static void main(String[] args) {
//        TimestampPriorityQueue tq = new TimestampPriorityQueue(3);
//
//        tq.insert(new Point(10, 1000));
//        tq.insert(new Point(20, 2000));
//        tq.insert(new Point(15, 1500));
//        tq.insert(new Point(25, 2500));
//
//        // 打印队列内容
//        long tmp = 2000;
//        for (Point point : tq.getQueue()) {
//            point.x = tmp--;
//            System.out.println("Value: " + point.x + ", Timestamp: " + point.y);
//        }
//
//        System.out.println(tq.queue.poll());
//    }
//}
