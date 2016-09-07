package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * MemProfileUtil
 */
public class MemProfileUtil {

    private static String[] PROFILES = {"LO","HI"};

    public static Map<String,String> mapOpts;
    static {
        mapOpts = new HashMap<String,String>();
        mapOpts.put(PROFILES[0],"-javaagent:./classmexer-0.0.3.jar -Xms800m -Xmx800m");
        mapOpts.put(PROFILES[1],"-javaagent:./classmexer-0.0.3.jar -Xms1800m -Xmx1800m");
    }
    public static Map<String,String> reduceOpts;
    static {
        reduceOpts = new HashMap<String,String>();
        reduceOpts.put(PROFILES[0],"-javaagent:./classmexer-0.0.3.jar -Xms800m -Xmx800m");
        reduceOpts.put(PROFILES[1],"-javaagent:./classmexer-0.0.3.jar -Xms1800m -Xmx1800m");
    }
    public static Map<String,Integer> mapMegaBytes;
    static {
        mapMegaBytes = new HashMap<String,Integer>();
        mapMegaBytes.put(PROFILES[0],1024);
        mapMegaBytes.put(PROFILES[1],2048);
    }
    public static Map<String,Integer> reduceMegaBytes;
    static {
        reduceMegaBytes = new HashMap<String,Integer>();
        reduceMegaBytes.put(PROFILES[0],1024);
        reduceMegaBytes.put(PROFILES[1],2048);
    }

    public static void setMemProfile(final String jobProfile , final Configuration conf) {
        if(!jobProfile.contains("/") || jobProfile.split("/").length != 2)
            throw new IllegalArgumentException("Uknnown profiles : " + jobProfile);
        setMemProfile(jobProfile.split("/")[0],jobProfile.split("/")[1],conf);
    }

    public static void setMemProfile(final String mapProfile,final String reduceProfile, final Configuration conf) {
        if(!mapProfile.equals(PROFILES[0]) && !mapProfile.equals(PROFILES[1]))
            throw new IllegalArgumentException("Uknown profile : " + mapProfile);
        conf.setInt("mapreduce.map.memory.mb",mapMegaBytes.get(mapProfile));
        conf.set("mapreduce.map.java.opts",mapOpts.get(mapProfile));
        if(!reduceProfile.equals(PROFILES[0]) && !reduceProfile.equals(PROFILES[1]))
            throw new IllegalArgumentException("Uknown profile : " + reduceProfile);
        conf.setInt("mapreduce.reduce.memory.mb", reduceMegaBytes.get(reduceProfile));
        conf.set("mapreduce.reduce.java.opts",reduceOpts.get(reduceProfile));
    }
}
