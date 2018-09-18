package com.neusoft;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NovelAnalysisMapper1 extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();
        String line = value.toString();
        String[] names = line.split(" ");
        set.addAll(Arrays.asList(names));
        for (String name:set){
            for (String other_name:set){
                if (name.equals(other_name)){
                    continue;
                }else {
                    context.write(new Text(name+","+other_name),new IntWritable(1));
                }
            }
        }
    }
}
