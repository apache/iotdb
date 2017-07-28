package cn.edu.thu.tsfiledb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Created by stefanie on 25/07/2017.
 */
public class TsfiledbTest {

    public static void main(String[] args) throws Exception {

        Connection connection = null;
        Statement statement = null;

        int startDevice = 0;
        int deviceNumber = 2;
        int sensorNumber = 2;
        int dataNumber = 5000000;

        String dateString = "2017-07-23";
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        Date d = f.parse(dateString);
        long startTime = d.getTime();
        //long endTime = System.currentTimeMillis();

        double startValue = 20;
        double endValue = 50;

        int insertCount = 0;

        String createTimeseriesSql = "CREATE TIMESERIES <timeseries> WITH DATATYPE=<datatype>, ENCODING=<encode>";
        String setStorageGroupSql = "SET STORAGE GROUP TO <prefixpath>";
        String insertDataSql = "INSERT INTO <timeseries> (timestamp, <sensor>) VALUES (<time>,<value>)";

        RandomNum r = new RandomNum();

        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
            statement = connection.createStatement();
            statement.setFetchSize(100000);

            for (int i = startDevice; i < startDevice + deviceNumber; i++) {

                // Create Timeseries
                for (int j = 0; j < sensorNumber; j++) {
                    String sql = createTimeseriesSql.replace("<timeseries>", "root.a.d" + i + ".s" + j)
                            .replace("<datatype>", "DOUBLE")
                            .replace("<encode>", "RLE");
//                    System.out.println(sql);
                    statement.execute(sql);
                }
                String sql = setStorageGroupSql.replace("<prefixpath>", "root.a.d" + i);
//                System.out.println(sql);
                statement.execute(sql);
            }

            for (int i = startDevice; i < startDevice + deviceNumber; i++) {

                // long time = r.getRandomLong(startTime, endTime);
                // Insert Data
                for (int k = 0; k < dataNumber; k++) {

                    startTime += 1000;
                    for (int j = 0; j < sensorNumber; j++) {
                        double value = r.getRandomDouble(startValue, endValue);
                        String sql = insertDataSql.replace("<timeseries>", "root.a.d" + i)
                                .replace("<sensor>", "s" + j)
                                .replace("<time>", startTime+"")
                                .replace("<value>", value+"");
//                        System.out.println(sql);
                        statement.execute(sql);
                        insertCount += 1;
                    }
                    //Thread.sleep(100);
                }
            }

            System.out.println("Total insert Record: " + insertCount);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                statement.close();
            }
            if(connection != null){
                connection.close();
            }
        }
    }
}



