package com.neusoft;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}
