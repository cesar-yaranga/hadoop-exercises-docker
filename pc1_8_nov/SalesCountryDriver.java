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

        private String itemType;
        private String country;
        private String itemDescription;
        private double amountPaid;
        private double itemPrice;

        public CustomWritable() {
            // Constructor sin argumentos requerido por Hadoop
        }

        public CustomWritable(String itemType, String country, String itemDescription, double amountPaid, double itemPrice) {
            this.itemType = itemType;
            this.country = country;
            this.itemDescription = itemDescription;
            this.amountPaid = amountPaid;
            this.itemPrice = itemPrice;
        }

        public String getItemType() {
            return itemType;
        }

        public String getCountry() {
            return country;
        }

        public String getItemDescription() {
            return itemDescription;
        }

        public double getAmountPaid() {
            return amountPaid;
        }

        public double getItemPrice() {
            return itemPrice;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(itemType);
            out.writeUTF(country);
            out.writeUTF(itemDescription);
            out.writeDouble(amountPaid);
            out.writeDouble(itemPrice);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            itemType = in.readUTF();
            country = in.readUTF();
            itemDescription = in.readUTF();
            amountPaid = in.readDouble();
            itemPrice = in.readDouble();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            String valueString = value.toString();
            String[] fields = valueString.split(",");

            if (!"item_type".equals(fields[0])) {
                String itemType = fields[0];
                String country = fields[3];
                String itemDescription = fields[5];
                double amountPaid = Double.parseDouble(fields[7]);

                // Validación para asegurarse de que itemPrice sea numérico
                double itemPrice;
                try {
                    itemPrice = Double.parseDouble(fields[4]);
                } catch (NumberFormatException e) {
                    //itemPrice en la entrada contiene un valor no numérico "Live at Vicar Street"
                    itemPrice = 0.0; // O asigna un valor predeterminado
                }

                CustomWritable customWritable = new CustomWritable(itemType, country, itemDescription, amountPaid, itemPrice);
                output.collect(new Text(itemType), customWritable);
            }
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String itemType = key.toString();
            String country = "";
            String itemDescription = "";
            double maxDifference = Double.MIN_VALUE;

            while (values.hasNext()) {
                CustomWritable value = values.next();
                String currentCountry = value.getCountry();
                String currentItemDescription = value.getItemDescription();
                double currentAmountPaid = value.getAmountPaid();
                double currentItemPrice = value.getItemPrice();
                double currentDifference = currentAmountPaid - currentItemPrice;

                if (currentDifference > maxDifference) {
                    maxDifference = currentDifference;
                    country = currentCountry;
                    itemDescription = currentItemDescription;
                }
            }

            String result = country + "\t" + itemDescription + "\t" + Double.toString(maxDifference);
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
