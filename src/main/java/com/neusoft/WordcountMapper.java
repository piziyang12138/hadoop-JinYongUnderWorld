package com.neusoft;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

public class WordcountMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    TreeSet<Integer> set = new TreeSet<>(Comparator.reverseOrder());
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word:words){
            set.add(Integer.parseInt(word));
        }
//        context.write(new Text(line),new IntWritable(1));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (int i = 0;i< 5;i++){
            if (!set.isEmpty()) {
                context.write(new IntWritable(set.pollFirst()), NullWritable.get());
            }
        }
    }
}

