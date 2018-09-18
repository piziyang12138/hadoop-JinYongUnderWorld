package com.neusoft;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class NovelAnalysisDriver3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtil.deleteDir("output4");
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.8.3");
        System.setProperty("HADOOP_USER_NAME", "root") ;
        run(args[0],args[1]+ File.separator + "Data1");
        for (int i = 1 ;i < Integer.parseInt(args[2]);i++){
            run(args[1]+ File.separator + "Data"+i + File.separator +"part-r-00000",args[1]+ File.separator + "Data"+(i+1));
        }
    }

    public static void run(String inputpath,String outputpath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(NovelAnalysisDriver3.class);
        job.setMapperClass(NovelAnalysisMapper3.class);
        job.setReducerClass(NovelAnalysisReducer3.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(inputpath));
        FileOutputFormat.setOutputPath(job,new Path(outputpath));

        boolean res = job.waitForCompletion(true);
    }
}
