package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SSGroupComparator extends WritableComparator {
    public SSGroupComparator(){
        super(TextTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextTuple t_1 = ((TextTuple) a);
        TextTuple t_2 = ((TextTuple) b);

        return (t_1.first.compareTo(t_2.first) != 0) ? t_1.first.compareTo(t_2.first)
                : t_1.second.compareTo(t_2.second);
    }
}