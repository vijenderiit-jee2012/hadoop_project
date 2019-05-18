package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<TextTuple, Text, Text, Text> {
    public void reduce(TextTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text new_key = new Text(key.first.toString() + key.second.toString() + key.third.toString());

        String val_cum = "";
        for (Text value : values) {
            System.out.println("KEY ->   "+key.first.toString() + "  " + key.second.toString() + "   " + key.third.toString());
            val_cum += value.toString() + ";";
        }
        context.write(new_key, new Text(val_cum));
    }
}
