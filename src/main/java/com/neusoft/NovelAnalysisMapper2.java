package com.neusoft;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NovelAnalysisMapper2 extends Mapper<LongWritable, Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] kv = line.split(",");
        context.write(new Text(kv[0]),new Text(kv[1]));
    }
}
