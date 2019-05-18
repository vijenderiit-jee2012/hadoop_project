package com.flipkart.flap;

import com.flipkart.flap.WordCount.WordJob;
import org.apache.hadoop.util.ToolRunner;

// sudo -u hdfs hadoop jar hadoop_project-1.0-SNAPSHOT-jar-with-dependencies.jar word_count root 6 /vijender/projects/word_count/input /vijender/projects/word_count/output
public class Main {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordJob(), args);
        System.exit(res);
    }
}