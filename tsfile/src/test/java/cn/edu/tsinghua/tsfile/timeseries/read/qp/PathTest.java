package cn.edu.tsinghua.tsfile.timeseries.read.qp;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.Test;

import static org.junit.Assert.*;

public class PathTest {
    private void testPath(Path path, String device, String measurement, String full) {
        assertEquals(device, path.getDeltaObjectToString());
        assertEquals(measurement, path.getMeasurementToString());
        assertEquals(full, path.getFullPath());
    }

    @Test
    public void construct() throws Exception {
        Path path = new Path("a.b.c");
        testPath(path, "a.b", "c", "a.b.c");
        path = new Path("c");
        testPath(path, "", "c", "c");
        path = new Path("");
        testPath(path, "", "", "");
    }

    @Test
    public void startWith() throws Exception {
        Path path = new Path("a.b.c");
        assertTrue(path.startWith(new Path("")));
        assertTrue(path.startWith(new Path("a")));
        assertTrue(path.startWith(new Path("a.b.c")));
    }

    @Test
    public void mergePath() throws Exception {
        Path prefix = new Path("a.b.c");
        Path suffix = new Path("d.e");
        Path suffix1 = new Path("");
        testPath(Path.mergePath(prefix, suffix), "a.b.c.d", "e", "a.b.c.d.e");
        testPath(Path.mergePath(prefix, suffix1), "a.b", "c", "a.b.c");
    }

    @Test
    public void addHeadPath() throws Exception {
        Path desc = new Path("a.b.c");
        Path head = new Path("d.e");
        Path head1 = new Path("");
        testPath(Path.addPrefixPath(desc, head), "d.e.a.b", "c", "d.e.a.b.c");
        testPath(Path.mergePath(desc, head1), "a.b", "c", "a.b.c");
    }

    @Test
    public void replace() throws Exception {
        Path src = new Path("a.b.c");
        Path rep1 = new Path("");
        Path rep2 = new Path("d");
        Path rep3 = new Path("d.e.f");
        Path rep4 = new Path("d.e.f.g");
        testPath(Path.replace(rep1,src), "a.b", "c", "a.b.c");
        testPath(Path.replace(rep2,src), "d.b", "c", "d.b.c");
        testPath(Path.replace(rep3,src), "d.e", "f", "d.e.f");
        testPath(Path.replace(rep4,src), "d.e.f", "g", "d.e.f.g");
    }

}