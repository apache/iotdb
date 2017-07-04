package cn.edu.thu.tsfiledb.sql;

import cn.edu.thu.tsfiledb.qp.logical.sys.LoadDataOperator;
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
                "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0","s1" ,"TOK_MULT_VALUE", "12345678", "-1011.666", "1231"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1)  values(12345678 , -1011.666, 1231)");
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
                "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0","s1" ,"TOK_MULT_VALUE","TOK_DATETIME" ,"2017-6-2T12:00:12+07:00", "-1011.666", "1231"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1)  values(2017-6-2T12:00:12+07:00, -1011.666, 1231)");
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
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UPDATE", "TOK_PATH", "d1", "s1", "TOK_VALUE", "-33000",
                "TOK_WHERE", "=", "TOK_PATH", "time", "TOK_DATETIME", "2017-6-2T12:00:12+07:00"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("update d1.s1 set value = -33000 where time = 2017-6-2T12:00:12+07:00");
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
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UPDATE", "TOK_PATH", "d1", "s1", "TOK_VALUE", "33000",
                "TOK_WHERE", "and", "<=", "TOK_PATH", "time", "TOK_DATETIME", "now", ">=", "TOK_PATH", "time", "1234567890102" ));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("update d1.s1 set value = 33000 where time <= now() and time >= 1234567890102");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
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
    public void loadData() throws ParseException{
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_LOAD", "'/user/data/input.csv'", "root", "laptop", "d1", "s1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("LOAD TIMESERIES '/user/data/input.csv' root.laptop.d1.s1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }
    
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
    public void selectCount() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_CLUSTER", "TOK_PATH",
                "s1", "count", "TOK_FROM", "TOK_PATH", "root", "vehicle", "d1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("select count(s1) from root.vehicle.d1");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void quit() throws ParseException, RecognitionException {
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

    @Test
    public void unlinkLabel() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UNLINK", "TOK_ROOT", "m1", "m2", "TOK_LABEL", "p2", "TOK_PROPERTY", "p1"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("unlink root.m1.m2 from p1.p2");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void linkLabel() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_LINK", "TOK_ROOT", "a", "b", "c", "TOK_LABEL", "labelhere", "TOK_PROPERTY", "propropro"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("link root.a.b.c to propropro.labelhere");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void deleteLabelProp() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DELETE", "TOK_LABEL", "label1021", "TOK_PROPERTY", "propropro"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("delete label label1021 from property propropro");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void addLabelProp() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_ADD", "TOK_LABEL", "label1021", "TOK_PROPERTY", "propropro"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("add label label1021 to property propropro");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }









    @Test
    public void grant() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_GRANT", "TOK_ROLE", "xm", "TOK_PRIVILEGES", "'create'", "'delete'","TOK_PATH", "root", "laptop", "d1", "s1sa"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("grant role xm privileges 'create','delete' on root.laptop.d1.s1sa");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void revoke() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_REVOKE", "TOK_ROLE", "admin", "TOK_USER", "xm"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("revoke admin from xm");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void createUser2() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_USER", "xm", "TOK_PASSWORD", "w123"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create user xm w123");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }

    @Test
    public void selectSum() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_CLUSTER", "TOK_PATH",
                "sensor_1", "total", "TOK_CLUSTER", "TOK_PATH", "sensor_2", "total", "TOK_FROM", "TOK_PATH", "device_1",
                "TOK_WHERE", "and", "<", "TOK_PATH", "time", "0.1", ">", "TOK_PATH", "freq", "10"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("select summ(sensor_1), total(sensor_2) from device_1 where time < 0.1 and freq > 10");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }







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






    @Test
    public void createUser1() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_USER", "xm", "TOK_PASSWORD", "192921"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("create user xm 192921");
        astTree = ParseUtils.findRootNonNullToken(astTree);
        recursivePrintSon(astTree, rec);

        int i = 0;
        while (i <= rec.size() - 1) {
            assertEquals(rec.get(i), ans.get(i));
            i++;
        }
    }



    @Test
    public void updatePassword() throws ParseException, RecognitionException {
        // template for test case
        ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_UPDATE", "TOK_UPDATE_PSWD", "'usezr++name'", "'newpassword'"));
        ArrayList<String> rec = new ArrayList<>();
        ASTNode astTree = ParseGenerator.generateAST("update user 'usezr++name' set password 'newpassword'");
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
    public void createProp() throws ParseException, RecognitionException {
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

    // Metadata
    @Test
    public void showMetadata() throws ParseException, RecognitionException {
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
    
    public void recursivePrintSon(Node ns, ArrayList<String> rec) {
        rec.add(ns.toString());
        if (ns.getChildren() != null) {
            for (Node nss : ns.getChildren()) {
                recursivePrintSon(nss, rec);
            }
        }
    }

}
