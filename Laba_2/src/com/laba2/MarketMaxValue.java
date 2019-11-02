package com.laba2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MarketMaxValue implements Writable {

    private double max;
    private int count;

    public MarketMaxValue() {

    }

    public MarketMaxValue(double max, int count) {
        this.max = max;
        this.count = count;
    }

    public double getMax() {
        return max;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(max);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        max = in.readDouble();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return new String(max + " ("+count+")");
    }
}
