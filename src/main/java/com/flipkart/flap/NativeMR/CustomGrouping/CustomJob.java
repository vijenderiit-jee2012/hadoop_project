package com.flipkart.flap.NativeMR.CustomGrouping;

import com.flipkart.flap.NativeMR.WordCount.WordJob;
import com.flipkart.flap.NativeMR.WordCount.WordMapper;
import com.flipkart.flap.NativeMR.WordCount.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.Arrays;

public class CustomJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        System.out.println("Argument ->  " + Arrays.toString(args));
        Job job = Job.getInstance(conf, args[0]);
        conf.set("mapred.job.queue.name", args[1]);

        Path inputPath = new Path(args[3]);
        Path outputPath = new Path(args[4]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(CustomJob.class);
        job.setOutputKeyClass(TextTuple.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Mapper.class);
        job.setPartitionerClass(SSPartitioner.class);
        job.setSortComparatorClass(SSSortComparator.class);
        job.setGroupingComparatorClass(SSGroupComparator.class);
        job.setReducerClass(Reducer.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}