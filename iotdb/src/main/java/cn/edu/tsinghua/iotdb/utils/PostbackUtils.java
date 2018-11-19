package cn.edu.tsinghua.iotdb.utils;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;

/**
 * @author lta
 */
public class PostbackUtils {
    private static String[] snapshotPaths = PostBackSenderDescriptor.getInstance().getConfig().snapshotPaths;

    /**
     * This method is to get a snapshot file path according to a tsfile path.
     * Due to multiple directories, it's necessary to make a snapshot in the same disk.
     * It's used by postback sender.
     *
     * @param filePath
     * @return
     */
    public static String getSnapshotFilePath(String filePath) {
        String[] name;
        String relativeFilePath;
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
            name = filePath.split(File.separator + File.separator);
            relativeFilePath = "data" + File.separator + name[name.length - 2] + File.separator + name[name.length - 1];
        } else {
            name = filePath.split(File.separator);
            relativeFilePath = "data" + File.separator + name[name.length - 2] + File.separator + name[name.length - 1];
        }
        String bufferWritePath = name[0];
        for (int i = 1; i < name.length - 2; i++)
            bufferWritePath = bufferWritePath + File.separator + name[i];
        for (String snapshotPath : snapshotPaths) {
            if (snapshotPath.startsWith(bufferWritePath)) {
                if (!new File(snapshotPath).exists())
                    new File(snapshotPath).mkdir();
                return snapshotPath + relativeFilePath;
            }
        }
        return null;
    }

    /**
     * Verify sending list is empty or not
     * It's used by postback sender.
     *
     * @param sendingFileList
     * @return
     */
    public static boolean isEmpty(Map<String, Set<String>> sendingFileList) {
        for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
            if (entry.getValue().size() != 0)
                return false;
        }
        return true;
    }

    /**
     * Verify IP address with IP white list which contains more than one IP segment.
     * It's used by postback sender.
     *
     * @param IPwhiteList
     * @param IPaddress
     * @return
     */
    public static boolean verifyIPSegment(String IPwhiteList, String IPaddress) {
        String[] IPsegments = IPwhiteList.split(",");
        for (String IPsegment : IPsegments) {
            int subnetMask = Integer.parseInt(IPsegment.substring(IPsegment.indexOf("/") + 1));
            IPsegment = IPsegment.substring(0, IPsegment.indexOf("/"));
            if (verifyIP(IPsegment, IPaddress, subnetMask))
                return true;
        }
        return false;
    }

    /**
     * Verify IP address with IP segment.
     *
     * @param IPsegment
     * @param IPaddress
     * @param subnetMark
     * @return
     */
    private static boolean verifyIP(String IPsegment, String IPaddress, int subnetMark) {
        String IPsegmentBinary = "";
        String IPaddressBinary = "";
        String[] IPsplits = IPsegment.split("\\.");
        DecimalFormat df = new DecimalFormat("00000000");
        for (String IPsplit : IPsplits) {
            IPsegmentBinary = IPsegmentBinary
                    + String.valueOf(df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
        }
        IPsegmentBinary = IPsegmentBinary.substring(0, subnetMark);
        IPsplits = IPaddress.split("\\.");
        for (String IPsplit : IPsplits) {
            IPaddressBinary = IPaddressBinary
                    + String.valueOf(df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
        }
        IPaddressBinary = IPaddressBinary.substring(0, subnetMark);
        if (IPaddressBinary.equals(IPsegmentBinary))
            return true;
        else
            return false;
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile() || file.list().length == 0) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            for (File f : files) {
                deleteFile(f);
                f.delete();
            }
        }
    }
}
