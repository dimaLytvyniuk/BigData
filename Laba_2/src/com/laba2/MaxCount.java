package com.laba2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MaxCount {
    public final static String START_DATE = "START_DATE";
    public final static String END_DATE = "END_DATE";
    public final static String SEARCHED_VALUE_FIRST = "SEARCHED_VALUE_FIRST";
    public final static String SEARCHED_VALUE_SECOND = "SEARCHED_VALUE_SECOND";

    public static void DoTask(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 6) {
            System.err.println("Usage: <hdfs://> <in> <out> <value1> <value2> <date>");
            System.exit(2);
        }

        setConfDoubleValue(conf, SEARCHED_VALUE_FIRST, args[3]);
        setConfDoubleValue(conf, SEARCHED_VALUE_SECOND, args[4]);
        setConfSearchedDate(conf, args[5]);

        FileSystem hdfs=FileSystem.get(new URI(args[0]), conf);
        Path resultFolder=new Path(args[2]);
        if(hdfs.exists(resultFolder))
            hdfs.delete(resultFolder, true);

        Job job = Job.getInstance(conf, "Max For Month Count");
        job.setJarByClass(MaxCount.class);
        job.setMapperClass(MaxCountMapper.class);
        job.setCombinerClass(MaxCountReducer.class);
        job.setReducerClass(MaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MarketMaxValue.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        boolean finishState = job.waitForCompletion(true);
        System.out.println("Job Running Time: " + (job.getFinishTime() - job.getStartTime()));

        System.exit(finishState ? 0 : 1);
    }

    public static void setConfSearchedDate(Configuration conf, String month) throws Exception {

        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM");
        Date parsingDate = ft.parse(month);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parsingDate);
        calendar.add(Calendar.MONTH, 1);

        conf.setLong(START_DATE, parsingDate.getTime());
        conf.setLong(END_DATE, calendar.getTimeInMillis());
    }

    public static void setConfDoubleValue(Configuration conf, String key, String stringValue) throws Exception {

        Double value = Double.parseDouble(stringValue);
        conf.setDouble(key, value);
    }
}
