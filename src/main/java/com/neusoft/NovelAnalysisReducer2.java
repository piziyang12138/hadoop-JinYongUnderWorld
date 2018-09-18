package com.neusoft;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NovelAnalysisReducer2 extends Reducer<Text, Text,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double count = 0;
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (Text str:values){
            list.add(str.toString());
            String[] nv = str.toString().split("\\s+");
            count += Integer.parseInt(nv[1]);
        }

        for (String text:list){
            String[] nv = text.split("\\s+");
            double number = Integer.parseInt(nv[1]);
            double scale =  number/count;
            sb.append(nv[0] + ":" + String.format("%.4f", scale) + ";");
        }
        sb.insert(0,key.toString() + "\t" +"0.1#");
        String res = sb.toString().substring(0,sb.length()-1);

        context.write(new Text(res),NullWritable.get());
    }
}
