package com.flipkart.flap;

import com.flipkart.flap.NativeMR.CustomGrouping.CustomJob;
import com.flipkart.flap.NativeMR.CustomGrouping.TextTuple;
import com.flipkart.flap.NativeMR.WordCount.WordJob;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

// sudo -u hdfs hadoop jar hadoop_project-1.0-SNAPSHOT-jar-with-dependencies.jar word_count root 6 /vijender/projects/word_count/input /vijender/projects/word_count/output
// sudo -u hdfs hadoop jar hadoop_project-1.0-SNAPSHOT-jar-with-dependencies.jar custom_grouping root 4 /vijender/projects/custom_grouping/input /vijender/projects/custom_grouping
// scp -i ~/Downloads/admin.rsa ./target/hadoop_project-1.0-SNAPSHOT-jar-with-dependencies.jar admin@10.34.81.91:~/
// mvn clean compile assembly:single
public class Main {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CustomJob(), args);
        System.exit(res);
    }
}