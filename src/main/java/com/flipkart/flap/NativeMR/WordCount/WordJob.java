package com.flipkart.flap.NativeMR.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.Arrays;

public class WordJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        System.out.println("Argument ->  " + Arrays.toString(args));
        Job job = Job.getInstance(conf, args[0]);
        conf.set("mapred.job.queue.name", args[1]);

        Path inputPath = new Path(args[3]);
        Path outputPath = new Path(args[4]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.getLocal(conf);

        job.setJarByClass(WordJob.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
