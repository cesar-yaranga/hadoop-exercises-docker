// package SalesCountry;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.w3c.dom.Text;

import java.util.*;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;

public class SalesCountryDriver {

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, IntWritable> {
        //private final static IntWritable one = new IntWritable(1);
        IntWritable numero;
        org.apache.hadoop.io.Text txttotal = new org.apache.hadoop.io.Text("cteSumaTotal");
        public void map(LongWritable key, org.apache.hadoop.io.Text value, OutputCollector<org.apache.hadoop.io.Text, IntWritable> output, Reporter reporter) throws IOException {
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");
            if(!"Price".equals(SingleCountryData[2])){
                numero = new IntWritable(Integer.parseInt(SingleCountryData[2]));
            }else{
                numero = new IntWritable(0);
            }
            output.collect(new org.apache.hadoop.io.Text(SingleCountryData[1] + SingleCountryData[3]), numero);
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<org.apache.hadoop.io.Text, IntWritable, org.apache.hadoop.io.Text, IntWritable> {
        public void reduce(org.apache.hadoop.io.Text t_key, Iterator<IntWritable> values, OutputCollector<org.apache.hadoop.io.Text, IntWritable> output, Reporter reporter) throws IOException {
            org.apache.hadoop.io.Text key = t_key;
            int frequencyForCountry = 0;
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                IntWritable value = (IntWritable) values.next();
                frequencyForCountry += value.get();
            }
            output.collect(key, new IntWritable(frequencyForCountry));
        }
    }

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SalesCountryDriver.class);
        // Set a name of the Job
        job_conf.setJobName("SalePerCountry");
        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);
        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(SalesMapper.class);
        job_conf.setReducerClass(SalesCountryReducer.class);
        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);
        // Set input and output directories using command line arguments, 
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
        my_client.setConf(job_conf);
        try {
            // Run the job 
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
