package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import org.junit.Assert;

import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.count;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static cn.edu.tsinghua.iotdb.service.TestUtils.sum;
import static org.junit.Assert.fail;

/**
 * Created by beyyes on 17/12/3.
 */
public class VerifyMain {

    private static final String d0s0 = "root.vehicle.d0.s0";
    private static final String d0s1 = "root.vehicle.d0.s1";
    private static final String d0s2 = "root.vehicle.d0.s2";
    private static final String d0s3 = "root.vehicle.d0.s3";
    private static final String d0s4 = "root.vehicle.d0.s4";

    private static final String d1s0 = "root.vehicle.d1.s0";
    private static final String d1s1 = "root.vehicle.d1.s1";

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};

    private static final String TIMESTAMP_STR = "Time";
    private static String[] booleanValue = new String[]{"true", "false"};

    public static void test_1(String[] args) {

        // SELECT max_value(s_76) FROM root.performf.group_4.d_41 WHERE root.performf.group_4.d_41.s_76 > 0.0
        // GROUP BY(250000ms, 1262275200000,[2010-01-02 02:59:59,2010-01-02 03:16:39])

        int cnt = 0;

        // insert large amount of data    time range : 3000 ~ 13600
        for (int time = 3000; time < 13600; time++) {

            if (time % 100 >= 20)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }


        // insert large amount of data    time range : 13700 ~ 24000
        for (int time = 13700; time < 24000; time++) {
            if (time % 70 >= 20)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        }

        for (int time = 2000; time < 2500; time++) {
            if (time >= 20)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }



        System.out.println("!!!!" + cnt);

        // buffwrite data, unsealed file
        for (int time = 100000; time < 101000; time++) {
            if (time % 20 >= 20)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        }

        for (int time = 200000; time < 201000; time++) {

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        }

        for (int time = 200900; time < 201000; time++) {
            cnt ++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
        }
        // statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");
        // statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

        System.out.println("====" + cnt);  // 16340


    }

    public static void test_2(String[] args) throws ClassNotFoundException, SQLException {

        // SELECT max_value(s_76) FROM root.performf.group_4.d_41 WHERE root.performf.group_4.d_41.s_76 > 0.0
        // GROUP BY(250000ms, 1262275200000,[2010-01-02 02:59:59,2010-01-02 03:16:39])

        String sql_1 = "﻿SELECT max_value(s_25) FROM root.performf.group_5.d_57 WHERE time > 2010-01-01 00:00:00 AND time < 2010-01-01 00:30:00";

        String sql_2 = "select count(s_92) from root.performf.group_3.d_33 where time > 2010-01-01 04:09:59 and time < 2010-01-01 04:26:39";


        String sql_3 = "SELECT max_value(s_76) FROM root.performf.group_4.d_41 WHERE root.performf.group_4.d_41.s_76 > 0.0  GROUP BY(250000ms, 1262275200000,[2010-01-02 02:59:59,2010-01-02 03:16:39])";

        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.23:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql_1);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + ","
                        + resultSet.getString(2);
                System.out.println(ans);
                //cnt++;
            }
            //Assert.assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void test_3(String[] args) {

        double sum0 = 0, sum1 = 0;
        long count0 = 0, count1 = 0;

        for (int time = 3000; time < 13600; time++) {
            sum0 += time % 100;
            count0 ++;
            if (time >= 3200) {
                sum1 += time % 17;
                count1 += 1;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
            sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
            sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
        }


        for (int time = 13700; time < 24000; time++) {
            sum0 += time % 70;
            count0 ++;
            if (time > 23000 && time < 100100) {
                count1 ++;
                sum1 += 11111111;
            } else {
                count1 ++;
                sum1 += time % 40;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        }



        // buffwrite data, unsealed file
        for (int time = 100000; time < 101000; time++) {
            sum0 += time % 20;
            count0 ++;
            if (time > 23000 && time < 100100) {
                count1 ++;
                sum1 += 11111111;
            } else {
                count1 ++;
                sum1 += time % 30;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        }


        // bufferwrite data, memory data
        for (int time = 200000; time < 201000; time++) {
            if (time >= 200900)
                break;
            sum0 -= time % 20;
            count0 ++;
            if (time > 23000 && time < 100100) {
                count1 ++;
                sum1 += 11111111;
            } else {
                count1 ++;
                sum1 -= time % 30;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        }

        //statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");

        for (int time = 2000; time < 2500; time++) {
            sum0 += time;
            count0 ++;
            if (time > 23000 && time < 100100) {
                count1 ++;
                sum1 += 11111111;
            } else {
                count1 ++;
                sum1 += time + 1;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }

        for (int time = 200900; time < 201000; time++) {
            sum0 += 6666;
            count0 ++;
            if (time > 23000 && time < 100100) {
                count1 ++;
                sum1 += 11111111;
            } else {
                count1 ++;
                sum1 += 7777;
            }

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
            sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time, booleanValue[time % 2]);
            sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
        }

        //statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

        System.out.println(count0 + " " + sum0 + " " + count1 + " " + sum1);
    }

    public static void main(String[] args) {

        // SELECT max_value(s_76) FROM root.performf.group_4.d_41 WHERE root.performf.group_4.d_41.s_76 > 0.0
        // GROUP BY(250000ms, 1262275200000,[2010-01-02 02:59:59,2010-01-02 03:16:39])

        int cnt = 0;

        // insert large amount of data    time range : 3000 ~ 13600
        for (int time = 3000; time < 13600; time++) {

            if (time % 100 < 111 && time >= 3200)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }


        // insert large amount of data    time range : 13700 ~ 24000
        for (int time = 13700; time < 24000; time++) {
            if (time % 70 < 111)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        }

        for (int time = 2000; time < 2500; time++) {
            if (time < 111 && time >= 3200)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }


        // buffwrite data, unsealed file
        for (int time = 100000; time < 101000; time++) {
            if (time % 20 < 111)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        }

        for (int time = 200000; time < 201000; time++) {
            cnt ++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        }

        for (int time = 200900; time < 201000; time++) {
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
        }
        // statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");
        // statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

        System.out.println("====" + cnt);


    }

}
