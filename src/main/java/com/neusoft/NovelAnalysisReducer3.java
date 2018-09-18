package com.neusoft;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NovelAnalysisReducer3 extends Reducer<Text, Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String nameList = "";
        double count = 0;
        for (Text text : values){
            String t = text.toString();
            if (t.charAt(0) == '#'){
                nameList = t;
            }else{
                count += Double.parseDouble(t);
            }
        }
        context.write(key,new Text(String.valueOf(count) + nameList));
    }


}
