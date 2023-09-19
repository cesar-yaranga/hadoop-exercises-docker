// package SalesCountry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesCountryDriver {

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
            
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");

            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat();
            String year = "";
            double ganancia = 0;
            DoubleWritable doubleGanancia = new DoubleWritable();

            // Para no utilizar los encabezados
            if(!"item_type".equals(SingleCountryData[0])) {

                // Aqui lo que debo hacer es coger la fecha
                // de la fecha extraigo el anho.

                // utc_date, artist_name, item_price, amount_paid

                // del anho concateno de esta forma:
                // key: concat(anho + artist_name)
                // value: ganancia = amount_paid - item_price

                try {
                    date = new SimpleDateFormat("MM/dd/yy HH:mm").parse(SingleCountryData[1]);
                    dateFormat = new SimpleDateFormat("yyyy");
                    year = dateFormat.format(date);

                    // Ahora debo realizar la resta
                    ganancia = Double.parseDouble(SingleCountryData[7]) - Double.parseDouble(SingleCountryData[4]);
                    doubleGanancia = new DoubleWritable(ganancia);

                } catch (ParseException ex) {
                    Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
                }

                // Nuestras llaves son utc_date y artist_name
                output.collect(new Text(year + ", " + SingleCountryData[8]), doubleGanancia);
            }
            
        }
    }
    

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
           
            Text key = t_key;
            double ganancia = 0;
            while (values.hasNext()) {
    
                DoubleWritable value = (DoubleWritable) values.next();
                
                // Contamos la cantidad de datos por cada llave (pais)
                ganancia += value.get();
            }
            output.collect(key, new DoubleWritable(ganancia));
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
        job_conf.setOutputValueClass(DoubleWritable.class);
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
