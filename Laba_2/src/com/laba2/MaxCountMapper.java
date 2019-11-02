package com.laba2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MaxCountMapper extends Mapper<Object, Text, Text, MarketMaxValue> {
    public static final Text MARKET_KEY1=new Text("EURUSD");
    public static final Text MARKET_KEY2=new Text("EURGBP");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Long startDate = conf.getLong(MaxCount.START_DATE, 0);
        Long endDate = conf.getLong(MaxCount.END_DATE, 0);
        Double firstValue = conf.getDouble(MaxCount.SEARCHED_VALUE_FIRST, 0);
        Double secondValue = conf.getDouble(MaxCount.SEARCHED_VALUE_SECOND, 0);

        String csvLine = value.toString();
        String[] csvField = csvLine.split(",");

        Long fistDate = parseDate(csvField[0]);
        Long secondDate = parseDate(csvField[7]);

        if (fistDate >= startDate && fistDate < endDate) {
            Double currentValue = Double.parseDouble(csvField[3]);

            if (currentValue.equals(firstValue)) {
                context.write(MARKET_KEY1, new MarketMaxValue(currentValue, 1));
            }
        }

        if (secondDate >= startDate && secondDate < endDate)
        {
            Double currentValue = Double.parseDouble(csvField[10]);

            if (currentValue.equals(secondValue)) {
                context.write(MARKET_KEY2, new MarketMaxValue(currentValue, 1));
            }
        }
    }

    private Long parseDate(String date) {
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy.MM.dd");
        Date parsingDate;

        try {
            parsingDate = ft.parse(date);
        } catch (Exception e) {
            parsingDate = new Date();
        }

        return parsingDate.getTime();
    }
}
