package cn.edu.thu.tsfiledb.sql;

import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.Node;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;
import org.antlr.runtime.RecognitionException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;


public class SQLParserTest {

	// Records Operations
    @Test
    public void createTimeseries() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_TIMESERIES", "TOK_ROOT", "laptop",
                "d0", "s2", "TOK_WITH", "TOK_DATATYPE", "FLOAT", "TOK_ENCODING", "RLE", "TOK_CLAUSE", "freq_encoding",
                "DFT", "TOK_CLAUSE", "write_main_freq", "true", "TOK_CLAUSE", "dft_pack_length", "300", "TOK_CLAUSE",
                "dft_rate", "0.4", "TOK_CLAUSE", "write_encoding", "false"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create timeseries root.laptop.d0.s2 with datatype=FLOAT" +
                ",encoding=RLE,freq_encoding=DFT,write_main_freq=true,dft_pack_length=300,dft_rate=0.4,write_encoding=false");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
    
    @Test
    public void createTimeseries2() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_TIMESERIES", "TOK_ROOT", "laptop",
                "d1", "s1", "TOK_WITH", "TOK_DATATYPE", "INT64", "TOK_ENCODING", "rle"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create timeseries root.laptop.d1.s1 with datatype=INT64,encoding=rle");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void setStorageGroup() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_SET", "TOK_STORAGEGROUP", "TOK_PATH", "root", "a", "b","c"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("set storage group to root.a.b.c");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void multiInsert() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_INSERT", "TOK_PATH", "root", "vehicle", "d0",
                "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0","s1" ,"TOK_MULT_VALUE", "12345678", "-1011.666", "'da#$%fa'"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1)  values(12345678 , -1011.666, 'da#$%fa')");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void multiInsert2() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_INSERT", "TOK_PATH", "root", "vehicle", "d0",
                "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0","s1" ,"TOK_MULT_VALUE","TOK_DATETIME" ,"now", "-1011.666", "1231"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1)  values(now() , -1011.666, 1231)");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
  
    @Test
    public void multiInsert3() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_INSERT", "TOK_PATH", "root", "vehicle", "d0",
                "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0","s1" ,"TOK_MULT_VALUE","TOK_DATETIME" ,"2017-6-2T12:00:12.000+07:00", "-1011.666", "1231"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1)  values(2017-6-2T12:00:12.000+07:00, -1011.666, 1231)");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
    @Test
    public void updateValueWithTimeFilter1() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_UPDATE", "TOK_PATH", "root", "vehicle", "d0", "s0", 
        		"TOK_VALUE", "-33000",
        		"TOK_WHERE", "=", "TOK_PATH", "time", "1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("UPDATE root.vehicle.d0.s0 SET VALUE = -33000 WHERE time = 1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void updateValueWithTimeFilter2() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_UPDATE", "TOK_PATH", "root", "vehicle", "d0", "s0", 
        		"TOK_VALUE", "-33000",
        		"TOK_WHERE", ">", "TOK_PATH", "time", "1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("UPDATE root.vehicle.d0.s0 SET VALUE = -33000 WHERE time > 1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void updateValueWithTimeFilter3() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_UPDATE", "TOK_PATH", "root", "vehicle", "d0", "s0", 
        		"TOK_VALUE", "-33000",
        		"TOK_WHERE", "<=", "TOK_PATH", "time", "1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("UPDATE root.vehicle.d0.s0 SET VALUE = -33000 WHERE time <= 1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void updateValueWithTimeFilter4() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_UPDATE", 
        		"TOK_PATH", "root", "vehicle", "d0", "s0", 
        		"TOK_PATH", "root", "vehicle", "d0", "s1", 
        		"TOK_VALUE", "-33000",
        		"TOK_WHERE", "<=", "TOK_PATH", "time", "1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("UPDATE root.vehicle.d0.s0,root.vehicle.d0.s1 SET VALUE = -33000 WHERE time <= 1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }    }
    
    @Test
    public void delete() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_PATH", "d1", "s1", "TOK_WHERE", "<",
                "TOK_PATH", "time", "TOK_DATETIME", "2016-11-16T16:22:33+08:00"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete from d1.s1 where time < 2016-11-16T16:22:33+08:00");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
        
    @Test
    public void delete2() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_PATH", "d1", "s1", "TOK_WHERE", "<",
                "TOK_PATH", "time", "TOK_DATETIME", "now"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete from d1.s1 where time < now()");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }   
  
    @Test
    public void delete3() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_PATH", "d1", "s1", "TOK_WHERE", "<",
                "TOK_PATH", "time",  "12345678909876"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete from d1.s1 where time < 12345678909876");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  

    @Test
    public void delete4() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_PATH", "d1", "s1", "TOK_PATH", "d2", "s3", "TOK_WHERE", "<",
                "TOK_PATH", "time", "TOK_DATETIME", "now"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete from d1.s1,d2.s3 where time < now()");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
    @Test
    public void delete5() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_PATH", "d1", "*", "TOK_PATH", "*", "s2", "TOK_WHERE", "<",
                "TOK_PATH", "time", "123456"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete from d1.*,*.s2 where time < 123456");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
//    @Test
//    public void loadData() throws ParseException{
//        // template for test case
//        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_LOAD", "'/user/data/input.csv'", "root", "laptop", "d1", "s1"));
//        ArrayList<String> rec = new ArrayList<>();
//        ASTNode astTree = ParseGenerator.generateAST("LOAD TIMESERIES '/user/data/input.csv' root.laptop.d1.s1");
//        astTree = ParseUtils.findRootNonNullToken(astTree);
//        recursivePrintSon(astTree, rec);
//
//        int i = 0;
//        while (i <= rec.size() - 1) {
//            assertEquals(rec.get(i), ans.get(i));
//            i++;
//        }
//    }
    
    @Test
    public void deleteTimeseires() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_TIMESERIES", "TOK_ROOT", "dt", "a", "b", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete timeseries root.dt.a.b.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
  
    @Test
    public void query1() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", 
        		"TOK_PATH", "device_1", "sensor_1", 
        		"TOK_PATH", "device_2", "sensor_2",
                "TOK_FROM", "TOK_PATH", "root", "vehicle", 
                "TOK_WHERE", "and", 
                "<", "TOK_PATH", "root", "laptop", "device_1", "sensor_1", "2000",
                ">", "TOK_PATH", "root", "laptop", "device_2", "sensor_2", "1000"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE root.laptop.device_1.sensor_1 < 2000 and root.laptop.device_2.sensor_2 > 1000");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
    
    @Test
    public void query2() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", 
        		"TOK_PATH", "device_1", "sensor_1", 
        		"TOK_PATH", "device_2", "sensor_2",
                "TOK_FROM", "TOK_PATH", "root", "vehicle", 
                "TOK_WHERE", "or", 
                "<", "TOK_PATH", "root", "laptop", "device_1", "sensor_1", "-2000",
                ">", "TOK_PATH", "time", "TOK_DATETIME" ,"now"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE root.laptop.device_1.sensor_1 < -2000 or time > now()");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
    @Test
    public void query3() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", 
        		"TOK_PATH", "device_1", "sensor_1", 
        		"TOK_PATH", "device_2", "sensor_2",
                "TOK_FROM", "TOK_PATH", "root", "vehicle", 
                "TOK_WHERE", "or", 
                "<", "TOK_PATH", "time", "1234567",
                ">", "TOK_PATH", "time", "TOK_DATETIME" ,"2017-6-2T12:00:12+07:00"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE time < 1234567 or time > 2017-6-2T12:00:12+07:00");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
    @Test
    public void query4() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", 
        		"TOK_PATH", "*", 
                "TOK_FROM", "TOK_PATH", "root", "vehicle", 
                "TOK_WHERE", "or", 
                "<", "TOK_PATH", "time", "1234567",
                ">", "TOK_PATH", "time", "TOK_DATETIME" ,"2017-6-2T12:00:12+07:00"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("SELECT * FROM root.vehicle WHERE time < 1234567 or time > 2017-6-2T12:00:12+07:00");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }  
    
    @Test
    public void aggregation1() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_QUERY", "TOK_SELECT", 
        		"TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s1", "count",
        		"TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "max_time",
        		"TOK_FROM", "TOK_PATH", "root", "vehicle", "d1",
        		"TOK_WHERE", "and", 
        		"<", "TOK_PATH", "root", "vehicle", "d1", "s1", "2000",
        		"<=", "TOK_PATH", "time", "TOK_DATETIME", "now"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("select count(s1),max_time(s2) from root.vehicle.d1 where root.vehicle.d1.s1 < 2000 and time <= now()");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void aggregation2() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_QUERY", "TOK_SELECT",
        		"TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "sum",
        		"TOK_FROM", "TOK_PATH", "root", "vehicle", "d1",
        		"TOK_WHERE", "or", 
        		"<", "TOK_PATH", "root", "vehicle", "d1", "s1", "2000",
        		">=", "TOK_PATH", "time", "1234567"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("select sum(s2) FROM root.vehicle.d1 WHERE root.vehicle.d1.s1 < 2000 or time >= 1234567");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void aggregation3() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList(
        		"TOK_QUERY", "TOK_SELECT",
        		"TOK_PATH", "s1", 
           	"TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "sum",
        		"TOK_FROM", "TOK_PATH", "root", "vehicle", "d1",
        		"TOK_WHERE", "or", 
        		"<", "TOK_PATH", "root", "vehicle", "d1", "s1", "2000",
        		">=", "TOK_PATH", "time", "1234567"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("select s1,sum(s2) FROM root.vehicle.d1 WHERE root.vehicle.d1.s1 < 2000 or time >= 1234567");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
    
    
    // Authority Operation
    @Test
    public void createUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_USER", "root", "TOK_PASSWORD", "mypwd"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create user root mypwd");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void dropUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DROP", "TOK_USER", "myname"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("drop user myname");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void updatePassword() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UPDATE", "TOK_UPDATE_PSWD", "myname", "newpassword"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("update user myname set password newpassword");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void createRole() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_ROLE", "admin"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create role admin");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void dropRole() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DROP", "TOK_ROLE", "admin"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("drop role admin");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void grantUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_GRANT", "TOK_USER", "myusername", "TOK_PRIVILEGES", "'create'", "'delete'","TOK_PATH", "root", "laptop", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("grant user myusername privileges 'create','delete' on root.laptop.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void grantRole() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_GRANT", "TOK_ROLE", "admin", "TOK_PRIVILEGES", "'create'", "'delete'","TOK_PATH", "root", "laptop", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("grant role admin privileges 'create','delete' on root.laptop.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void revokeUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_REVOKE", "TOK_USER", "myusername", "TOK_PRIVILEGES", "'create'", "'delete'","TOK_PATH", "root", "laptop", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("revoke user myusername privileges 'create','delete' on root.laptop.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void revokeRole() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_REVOKE", "TOK_ROLE", "admin", "TOK_PRIVILEGES", "'create'", "'delete'","TOK_PATH", "root", "laptop", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("revoke role admin privileges 'create','delete' on root.laptop.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
    
    @Test
    public void grantRoleToUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_GRANT", "TOK_ROLE", "admin", "TOK_USER", "Tom"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("grant admin to Tom");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }   
    
    @Test
    public void revokeRoleFromUser() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_REVOKE", "TOK_ROLE", "admin", "TOK_USER", "Tom"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("revoke admin from Tom");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void quit() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Collections.singletonList("TOK_QUIT"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("quit");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    // Property Tree Operation
    @Test
    public void createProp() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_PROPERTY", "myprop"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create property myprop");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void addLabelToProp() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_ADD", "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("ADD LABEL myLabel TO PROPERTY myProp");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }    
    
    @Test
    public void delelteLabelFromProp() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_LABEL", "myLable", "TOK_PROPERTY", "myProp"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("DELETE LABEL myLable FROM PROPERTY myProp");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }   
   
    @Test
    public void linkLabel() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_LINK", "TOK_ROOT", "a", "b", "c", "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("link root.a.b.c to myProp.myLabel");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    @Test
    public void unlinkLabel() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UNLINK", "TOK_ROOT", "m1", "m2", "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("unlink root.m1.m2 from myProp.myLabel");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    // Metadata
    @Test
    public void showMetadata() throws ParseException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_SHOW_METADATA"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("show metadata");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
    // others
    @Test
    public void describePath() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DESCRIBE", "TOK_PATH", "root"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("describe root");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    public void recursivePrintSon(Node ns, ArrayList<String> rec) {
        rec.add(ns.toString());
        if (ns.getChildren() != null) {
            for (Node nss : ns.getChildren()) {
                recursivePrintSon(nss, rec);
            }
        }
    }

}
