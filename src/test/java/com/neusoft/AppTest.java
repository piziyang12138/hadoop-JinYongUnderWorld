package com.neusoft;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeSet;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    Configuration conf = null;
    FileSystem fileSystem = null;
    @Before
    public void init() throws Exception{
//        conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop01:8020");
//        fileSystem = FileSystem.get(conf);
    }

    @Test
    public void shouldAnswerWithTrue()
    {
        String a = "8.850592905322607E-4";
        System.out.println(Float.parseFloat(a)+1);
    }

    @Test
    public void testUpload() throws Exception{
        for (int i = 0;i < 70;i++) {
            fileSystem.copyFromLocalFile(new Path("d:/a.jpg"), new Path("/a"+i+".jpg"));
        }
        fileSystem.close();
    }
}
