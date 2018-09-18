package com.neusoft;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NovelAnalysisMapper4 extends Mapper<LongWritable, Text,Text, Text> {
    int count = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int index_t = line.indexOf("\t");
        int index_j = line.indexOf("#");
        int index_dollar = line.indexOf("$");
        String name = line.substring(0,index_t);
        if (index_dollar != -1){
            name = line.substring(index_dollar+1,index_t);
            context.write(new Text(name),new Text("$"+line.substring(0,index_dollar)));
        }else{
            count++;
            context.write(new Text(name),new Text("$"+String.valueOf(count)));
        }

        context.write(new Text(name),new Text("@"+line.substring(index_t)));
        String maxName = "";
        float maxRelation = Float.MIN_VALUE;
        float tmp;
        String[] names = line.substring(index_j+1).split(";");
        for (String name_value:names){
            String[] nv = name_value.split(":");
            tmp = maxRelation;
            maxRelation = Math.max(maxRelation,Float.parseFloat(nv[1]));
            if (tmp < maxRelation){
                maxName = nv[0];
            }
        }
        context.write(new Text(name),new Text("!"+maxName));
    }


}
