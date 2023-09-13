// package SalesCountry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;

public class SalesCountryDriver {

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
    
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            if (key.get() == 0) {
                return;
            }
            String valueString = value.toString();
            String[] singleRowData = valueString.split(",");
            output.collect(new Text(singleRowData[7] + "," + singleRowData[5] + "," + singleRowData[3]), one);
        }
    }
    
    public static class SalesMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    
            String[] rowData = value.toString().split("\t");
            String[] rowValue = rowData[0].split(",");
            int valueInt = Integer.parseInt(rowData[1]);
            output.collect(new Text(rowValue[0] + "," + rowValue[1]), new Text(rowValue[2] + "," + valueInt));
        }
    }
    

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            Text key = t_key;
            int frequencyForCountry = 0;
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                IntWritable value = (IntWritable) values.next();
                frequencyForCountry += value.get();
    
            }
            output.collect(key, new IntWritable(frequencyForCountry));
        }
    }
    
    public static class SalesReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
        public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Text key = t_key;
            int max = 0;
            String argmax = "";
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                Text value = (Text) values.next();
                String[] vals = value.toString().split(",");
                int price = Integer.parseInt(vals[1]);
                if (price > max) {
                    max = price;
                    argmax = vals[0];
                }
            }
            output.collect(key, new Text(argmax));
        }
    }
    

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf1 = new JobConf(SalesCountryDriver.class);
        job_conf1.setJobName("Sale1");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(IntWritable.class);
        job_conf1.setMapperClass(SalesMapper.class);
        job_conf1.setReducerClass(SalesCountryReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
        my_client.setConf(job_conf1);
        //#########################################

        JobClient my_client2 = new JobClient();
        JobConf job_conf2 = new JobConf(SalesCountryDriver.class);
        job_conf2.setJobName("Sale2");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(SalesMapper2.class);
        job_conf2.setReducerClass(SalesReducer2.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        // FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
        // FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));
        FileInputFormat.setInputPaths(job_conf2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[1]));

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();  
        }

    }
}
