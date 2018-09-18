package com.neusoft;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {
    public static void main(String[] args) throws Exception {
        FileUtil.deleteDir("output");
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.8.3");
        System.setProperty("HADOOP_USER_NAME", "root") ;

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop01:8020");
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordcountDriver.class);

        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}

