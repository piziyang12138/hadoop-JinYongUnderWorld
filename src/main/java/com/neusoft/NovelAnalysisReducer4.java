package com.neusoft;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NovelAnalysisReducer4 extends Reducer<Text, Text,Text, Text> {
    Map<String,String> label_map = new HashMap<>();
    Map<String,String> relation_map = new HashMap<>();
    Map<String,String> origin_map = new HashMap<>();
//    Map<String,String> update_label_map = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text:values){
            String str = text.toString();
            if (str.charAt(0) == '$'){
                label_map.put(key.toString(),str.substring(1));
            }
            if (str.charAt(0) == '@'){
                relation_map.put(key.toString(),str.substring(1));
            }
            if (str.charAt(0) == '!'){
                origin_map.put(key.toString(),str.substring(1));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String,String> entry : label_map.entrySet()){
            String name = entry.getKey();
            String origin_name = origin_map.get(name);
            String target_label = label_map.get(origin_name);
            label_map.put(name,target_label);
            context.write(new Text(target_label+"$"+name),new Text(relation_map.get(name)));
        }
    }
}
