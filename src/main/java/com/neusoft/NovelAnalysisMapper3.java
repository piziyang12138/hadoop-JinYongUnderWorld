package com.neusoft;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NovelAnalysisMapper3 extends Mapper<LongWritable, Text,Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int index_dollar = line.indexOf("$");
        if (index_dollar != -1){
            line = line.substring(index_dollar+1);
        }
        int index_t = line.indexOf("\t");
        int index_j = line.indexOf("#");
        double PR = Double.parseDouble(line.substring(index_t+1,index_j));
        String name = line.substring(0,index_t);
        String names = line.substring(index_j+1);
        for (String name_value:names.split(";")){
            String[] nv = name_value.split(":");
            double relation = Double.parseDouble(nv[1]);
            double cal = PR * relation;
            context.write(new Text(nv[0]),new Text(String.valueOf(cal)));
        }
        context.write(new Text(name),new Text("#"+line.substring(index_j+1)));
    }


}
