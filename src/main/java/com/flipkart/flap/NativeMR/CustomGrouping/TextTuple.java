package com.flipkart.flap.NativeMR.CustomGrouping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextTuple implements WritableComparable<TextTuple> {
    public Text first, second, third;

    public TextTuple(){
        first = new Text();
        second = new Text();
        third = new Text();
    }

    public TextTuple(Text first, Text second, Text third){
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public int compareTo(TextTuple other) {
        return this.first.compareTo(other.first) != 0 ? this.first.compareTo(other.first)
                : this.second.compareTo(other.second) != 0 ? this.second.compareTo(other.second)
                : this.third.compareTo(other.third);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.first.write(dataOutput);
        this.second.write(dataOutput);
        this.third.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first.readFields(dataInput);
        this.second.readFields(dataInput);
        this.third.readFields(dataInput);
    }
}
