package org.apache.iotdb.db.integration.tri;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.statistics.QuickHull;
import org.apache.iotdb.tsfile.file.metadata.statistics.QuickHullPoint;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.BitSet;

public class MyTest_ch {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test1() throws Exception {
    System.out.println("Quick Hull Test");
    ArrayList<QuickHullPoint> points = new ArrayList<>();
    points.add(new QuickHullPoint(0, 0, 0));
    points.add(new QuickHullPoint(1, 3, 1));
    points.add(new QuickHullPoint(2, 2, 2));
    points.add(new QuickHullPoint(3, 2, 3));
    points.add(new QuickHullPoint(4, 0, 4));
    points.add(new QuickHullPoint(5, 2, 5));
    points.add(new QuickHullPoint(100, 2, 6));

    BitSet bitSet1 = QuickHull.quickHull(points);
    System.out.println(bitSet1);
    //    System.out.println(bitSet1.size());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(bitSet1);
    oos.flush();
    byte[] bytes = baos.toByteArray();
    //    byte[] bytes = bitSet1.toByteArray();
    System.out.println(bytes.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    BitSet bitSet = (BitSet) ois.readObject();
    System.out.println(bitSet);
    //    System.out.println(bitSet.size());
    Assert.assertTrue(bitSet.get(1));
    Assert.assertFalse(bitSet.get(3));
  }

  //  public static void main(String args[]) throws Exception {
  //
  //    System.out.println("Quick Hull Test");
  ////    Scanner sc = new Scanner(System.in);
  ////    System.out.println("Enter the number of points");
  ////    int N = sc.nextInt();
  //
  //    ArrayList<QuickHullPoint> points = new ArrayList<>();
  ////    System.out.println("Enter the coordinates of each points: <x> <y>");
  ////    for (int i = 0; i < N; i++) {
  ////      int x = sc.nextInt();
  ////      int y = sc.nextInt();
  ////      Point e = new Point(x, y);
  ////      points.add(i, new QuickHullPoint(x,y,i));
  ////    }
  //    points.add(new QuickHullPoint(0,0,0));
  //    points.add(new QuickHullPoint(1,3,1));
  //    points.add(new QuickHullPoint(2,2,2));
  //    points.add(new QuickHullPoint(3,2,3));
  //    points.add(new QuickHullPoint(4,0,4));
  //    points.add(new QuickHullPoint(5,2,5));
  //
  ////    QuickHull qh = new QuickHull();
  //    BitSet bitSet1 = QuickHull.quickHull(points);
  ////    System.out
  ////        .println("The points in the Convex hull using Quick Hull are: ");
  ////    for (int i = 0; i < p.size(); i++) {
  ////      System.out.println("(" + p.get(i).x + ", " + p.get(i).y + ")");
  ////    }
  //    System.out.println(bitSet1.toString());
  //    System.out.println(bitSet1.size());
  ////    sc.close();
  //
  ////    BitSet bits = new BitSet(100000);
  ////    System.out.println(bits.size());
  ////    bits.set(2);
  //    ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //    ObjectOutputStream oos = new ObjectOutputStream(baos);
  //    oos.writeObject(bitSet1);
  //    oos.close();
  //    byte[] bytes = baos.toByteArray();
  //    System.out.println(bytes.length);
  //    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
  //    ObjectInputStream ois = new ObjectInputStream(bais);
  //    BitSet bitSet = (BitSet) ois.readObject();
  //    System.out.println(bitSet);
  //    System.out.println(bitSet.get(1));
  //    System.out.println(bitSet.get(3));
  //    System.out.println(bitSet.size());
  //  }
}
