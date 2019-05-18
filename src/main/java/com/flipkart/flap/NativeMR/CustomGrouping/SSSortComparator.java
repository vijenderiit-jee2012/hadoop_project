package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SSSortComparator extends WritableComparator {
    public SSSortComparator(){
        super(TextTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}
