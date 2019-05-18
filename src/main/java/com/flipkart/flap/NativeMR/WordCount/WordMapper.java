package com.flipkart.flap.NativeMR.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String[] word_stream = text.toString().replace("\n", "").split(",");
        System.out.println("Stream ->  "  + Arrays.toString(word_stream));

        for (String each_word : word_stream){
            word.set(each_word.toLowerCase());
            context.write(word, one);
        }
    }
}