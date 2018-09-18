package com.neusoft;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NovelAnalysisDriver1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtil.deleteDir("output2");
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.8.3");
        System.setProperty("HADOOP_USER_NAME", "root") ;
        Configuration conf = new Configuration();
//        conf.set("namelist",args[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(NovelAnalysisDriver1.class);
        job.setMapperClass(NovelAnalysisMapper1.class);
        job.setReducerClass(NovelAnalysisReducer1.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
