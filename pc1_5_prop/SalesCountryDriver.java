// package SalesCountry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import java.util.*;

import org.apache.hadoop.io.Writable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.DoubleWritable;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {
        // Esta clase tiene tres atributos
        private Text country;

        private DoubleWritable item_price;
        private DoubleWritable amount_paid;

        public CustomWritable() {
            country = new Text();

            item_price = new DoubleWritable();
            amount_paid = new DoubleWritable();
        }

        public CustomWritable(Text country, DoubleWritable item_price, DoubleWritable amount_paid) {
            this.country = country;

            this.item_price = item_price;
            this.amount_paid = amount_paid;
        }

        // INICIO: Getters and Setters
        public Text getCountry() {
            return country;
        }

        public void setCountry(Text country) {
            this.country = country;
        }
        
        public DoubleWritable getItem_price() {
            return item_price;
        }

        public void setItem_price(DoubleWritable item_price) {
            this.item_price = item_price;
        }

        public DoubleWritable getAmount_paid() {
            return amount_paid;
        }

        public void setAmount_paid(DoubleWritable amount_paid) {
            this.amount_paid = amount_paid;
        }
        // FIN: Getters and Setters

        public void readFields(DataInput in) throws IOException {
            country.readFields(in);

            item_price.readFields(in);
            amount_paid.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            country.write(out);

            item_price.write(out);
            amount_paid.write(out);
        }

        @Override
        public String toString() {
            // return utc_date.toString() + "\t" + country.toString() + "\t" + artist_name.toString() + "\t" + item_type.toString();
            return country.toString();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        CustomWritable customWritable;

        Text textCountry;

        DoubleWritable doubleItem_price;
        DoubleWritable doubleAmount_paid;
        
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");
            
            // Filtro solo para EEUU
            if(!"item_type".equals(SingleCountryData[0])) {
                textCountry = new Text(SingleCountryData[8]);

                doubleItem_price = new DoubleWritable(Double.parseDouble(SingleCountryData[4]));
                doubleAmount_paid = new DoubleWritable(Double.parseDouble(SingleCountryData[7]));

                customWritable = new CustomWritable(textCountry, doubleItem_price, doubleAmount_paid);

                // Nuestras llaves son los paises
                output.collect(new Text(SingleCountryData[8]), customWritable);
            }
        }
    }
    
    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

            Text key = t_key;

            String country = "";

            double item_price = 0;
            double amount_paid = 0;

            double media_geometrica = 1;
            double iterador = 0;

            while (values.hasNext()) {
    
                CustomWritable value = (CustomWritable) values.next();

                Logger.getLogger(value.toString()).log(Level.SEVERE, "Mensaje de registro", value);

                Text textCountry = (Text) value.getCountry();
                
                DoubleWritable doubleItem_price = (DoubleWritable) value.getItem_price();
                DoubleWritable doubleAmount_paid = (DoubleWritable) value.getAmount_paid();

                country = textCountry.toString();

                item_price = doubleItem_price.get();
                amount_paid = doubleAmount_paid.get();

                iterador += 1;
                
                media_geometrica *= amount_paid;
            }

            media_geometrica = Math.pow(media_geometrica, 1.0 / iterador);

            // output.collect(key, new CustomWritable(new Text(utc_dateMaximo), new Text(countryMaximo), new Text(artist_nameMaximo), new Text(item_typeMaximo)));
            output.collect(key, new DoubleWritable(media_geometrica));
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
        job_conf.setOutputValueClass(CustomWritable.class);
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
