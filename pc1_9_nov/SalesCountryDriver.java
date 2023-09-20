//package SalesCountryDriver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Text;

import java.util.*;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {

        private double maxAmountPaid;
        private String country;
        private String artistName;

        public CustomWritable() {
            // Constructor sin argumentos requerido por Hadoop
            this.maxAmountPaid = Double.MIN_VALUE;
            this.artistName = "";
            this.country = "";
        }

        public CustomWritable(Double maxAmountPaid, String country, String artistName) {
            this.maxAmountPaid = maxAmountPaid;
            this.artistName = artistName;
            this.country = country;
        }

        public double getMaxAmountPaid() {
            return maxAmountPaid;
        }

        public String getCountry() {
            return country;
        }

        public String getArtistName() {
            return artistName;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(maxAmountPaid);
            out.writeUTF(country);
            out.writeUTF(artistName);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            maxAmountPaid = in.readDouble();
            country = in.readUTF();
            artistName = in.readUTF();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            String valueString = value.toString();
            String[] fields = valueString.split(",");

            if (!"item_type".equals(fields[0])) {
                String itemType = fields[0];
                double amountPaid = Double.parseDouble(fields[7]);
                String country = fields[3];
                String artistName = fields[8];

                CustomWritable customWritable = new CustomWritable(amountPaid, country, artistName);
                output.collect(new Text(itemType), customWritable);
            }
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double maxAmountPaid = Double.MIN_VALUE;
            String country = "";
            String artistName = "";

            while (values.hasNext()) {
                CustomWritable value = values.next();
                double currentMaxAmountPaid = value.getMaxAmountPaid();
                String currentCountry = value.getCountry();
                String currentArtistName = value.getArtistName();

                if (currentMaxAmountPaid > maxAmountPaid) {
                    maxAmountPaid = currentMaxAmountPaid;
                    country = currentCountry;
                    artistName = currentArtistName;
                }
            }
            String result = country + "\t" + Double.toString(maxAmountPaid) + "\t" + artistName;
            output.collect(key, new Text(result));
        }
    }

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf = new JobConf(SalesCountryDriver.class);

        job_conf.setJobName("SalesCalculator");
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(CustomWritable.class);
        job_conf.setMapperClass(SalesMapper.class);
        job_conf.setReducerClass(SalesCountryReducer.class);
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

        my_client.setConf(job_conf);

        try {
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
