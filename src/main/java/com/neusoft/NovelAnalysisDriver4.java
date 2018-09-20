package com.neusoft;


import lpa.LPAIteration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NovelAnalysisDriver4 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtil.deleteDir("output5");
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.8.3");
        System.setProperty("HADOOP_USER_NAME", "root") ;
        run(args[0],args[1]);
        for (int i = 1;i < 6;i++){
            run("output5\\Data"+i+"\\part-r-00000","output5\\Data" +(i+1));
        }
    }
    public static void run(String input,String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(NovelAnalysisDriver4.class);
        job.setMapperClass(NovelAnalysisMapper5.class);
        job.setReducerClass(NovelAnalysisReducer5.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        boolean res = job.waitForCompletion(true);
    }
}
