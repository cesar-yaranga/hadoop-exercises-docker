// package SalesCountry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.*;

import org.apache.hadoop.io.Writable;
import java.text.ParseException;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {
        // Esta clase tiene tres atributos
        // utc_date
        // country
        // artist_name
        // item_type
        private Text utc_date;
        private Text country;
        private Text artist_name;
        private Text item_type;

        public CustomWritable() {
            utc_date = new Text();
            country = new Text();
            artist_name = new Text();
            item_type = new Text();
        }

        public CustomWritable(Text utc_date, Text country, Text artist_name, Text item_type) {
            this.utc_date = utc_date;
            this.country = country;
            this.artist_name = artist_name;
            this.item_type = item_type;
        }

        // INICIO: Getters and Setters
        public Text getUtc_date() {
            return utc_date;
        }

        public void setUtc_date(Text utc_date) {
            this.utc_date = utc_date;
        }

        public Text getCountry() {
            return country;
        }

        public void setCountry(Text country) {
            this.country = country;
        }

        public Text getArtist_name() {
            return artist_name;
        }

        public void setArtist_name(Text artist_name) {
            this.artist_name = artist_name;
        }

        public Text getItem_type() {
            return item_type;
        }

        public void setItem_type(Text item_type) {
            this.item_type = item_type;
        }
        // FIN: Getters and Setters

        public void readFields(DataInput in) throws IOException {
            utc_date.readFields(in);
            country.readFields(in);
            artist_name.readFields(in);
            item_type.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            utc_date.write(out);
            country.write(out);
            artist_name.write(out);
            item_type.write(out);
        }

        @Override
        public String toString() {
            // return utc_date.toString() + "\t" + country.toString() + "\t" + artist_name.toString() + "\t" + item_type.toString();
            return country.toString() + " - " + artist_name.toString() + " - " + item_type.toString();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        CustomWritable customWritable;
        Text textUtc_date;
        Text textCountry;
        Text textArtist_name;
        Text textItem_type;
        
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");
            
            String substring = "the";
            
            // Para no utilizar los encabezados
            if(!"item_type".equals(SingleCountryData[0])) {
                if(SingleCountryData[8].contains(substring)) {
                    textUtc_date = new Text(SingleCountryData[1]);
                    textCountry = new Text(SingleCountryData[3]);
                    textArtist_name = new Text(SingleCountryData[8]);
                    textItem_type = new Text(SingleCountryData[0]);

                    customWritable = new CustomWritable(textUtc_date, textCountry, textArtist_name, textItem_type);
                    
                    // Nuestras llaves son los paises
                    output.collect(new Text(SingleCountryData[1]), customWritable);
                }
            }
        }
    }
    
    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, CustomWritable> {

        @Override
        public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {

            Text key = t_key;

            String utc_date = "";
            String country = "";
            String artist_name = "";
            String item_type = "";

            long msMaximo = 0;

            String utc_dateMaximo = "";
            String countryMaximo = "";
            String artist_nameMaximo = "";
            String item_typeMaximo = "";

            while (values.hasNext()) {
    
                CustomWritable value = (CustomWritable) values.next();

                Text textUtc_date = (Text) value.getUtc_date();
                Text textCountry = (Text) value.getCountry();
                Text textArtist_name = (Text) value.getArtist_name();
                Text textItem_type = (Text) value.getItem_type();

                utc_date = textUtc_date.toString();
                country = textCountry.toString();
                artist_name = textArtist_name.toString();
                item_type = textItem_type.toString();

                try {
                    Date dateCreacion = new SimpleDateFormat("MM/dd/yy HH:mm").parse(utc_date);
                            
                    long ms =  dateCreacion.getTime();
    
                    if (ms > msMaximo) {
                        msMaximo = ms;
    
                        utc_dateMaximo = utc_date;
                        countryMaximo = country;
                        artist_nameMaximo = artist_name;
                        item_typeMaximo = item_type;
                    }
    
                } catch (ParseException ex) {
                    Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            // output.collect(key, new CustomWritable(new Text(utc_dateMaximo), new Text(countryMaximo), new Text(artist_nameMaximo), new Text(item_typeMaximo)));
            output.collect(key, new CustomWritable(new Text(utc_date), new Text(country), new Text(artist_name), new Text(item_type)));
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
