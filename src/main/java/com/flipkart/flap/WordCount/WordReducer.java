package com.flipkart.flap.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int sum = 0;
        System.out.print("Word : "+word );
        for (IntWritable count : counts) {
            System.out.print("  "+count);
            sum += count.get();
        }
        System.out.println();
        context.write(word, new IntWritable(sum));
    }
}
