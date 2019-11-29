package com.laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxCountReducer extends Reducer<Text, MarketMaxValue, Text, MarketMaxValue> {

    public void reduce(Text key, Iterable<MarketMaxValue> values, Context context) throws IOException, InterruptedException {
        double max = 0;
        int count = 0;

        for(MarketMaxValue maxValue: values) {
            max = maxValue.getMax();
            count += maxValue.getCount();
        }

        context.write(key, new MarketMaxValue(max, count));
    }
}
