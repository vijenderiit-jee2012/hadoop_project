package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SSPartitioner extends Partitioner<TextTuple, Object>{
    @Override
    public int getPartition(TextTuple textTuple, Object o, int partitions) {
        Text first = textTuple.first;
        return first.hashCode() % partitions;
    }
}