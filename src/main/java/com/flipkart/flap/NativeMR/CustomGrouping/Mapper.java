package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, TextTuple, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        TextTuple textTuple = new TextTuple(new Text(values[0]), new Text(values[1]), new Text(values[2]));
        context.write(textTuple, value);
    }
}
