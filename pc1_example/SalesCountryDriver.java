// package SalesCountry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Date;
import org.apache.hadoop.io.Text;

import java.util.*;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {

        private Text nombre;
        private Text creacionCuenta;
        private Text ultimoLogueo;



        public CustomWritable() {
            nombre = new Text();
            creacionCuenta = new Text();
            ultimoLogueo = new Text();


        }

        public CustomWritable(Text nombre, Text creacionCuenta, Text ultimoLogueo) {
            this.nombre = nombre;
            this.creacionCuenta = creacionCuenta;
            this.ultimoLogueo = ultimoLogueo;

        }

        public Text getNombre() {
            return nombre;
        }

        public void setNombre(Text nombre) {
            this.nombre = nombre;
        }

        public Text getCreacionCuenta() {
            return creacionCuenta;
        }

        public void setCreacionCuenta(Text creacionCuenta) {
            this.creacionCuenta = creacionCuenta;
        }

        public Text getUltimoLogueo() {
            return ultimoLogueo;
        }

        public void setUltimoLogueo(Text ultimoLogueo) {
            this.ultimoLogueo = ultimoLogueo;
        }


        public void readFields(DataInput in) throws IOException {
            nombre.readFields(in);
            creacionCuenta.readFields(in);
            ultimoLogueo.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            nombre.write(out);
            creacionCuenta.write(out);
            ultimoLogueo.write(out);
        }

        @Override
        public String toString() {
            return nombre.toString() + "\t" + creacionCuenta.toString() + "\t" + ultimoLogueo.toString();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        // Para el conteo
        private final static IntWritable one = new IntWritable(1);
            
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            
            String valueString = value.toString();
            String[] SingleCountryData = valueString.split(",");
            
            // Para no utilizar los encabezados
            if( !"item_type".equals(SingleCountryData[0]) ) {
                // Nuestras llaves son los paises
                output.collect(new Text(SingleCountryData[3] + ", " + SingleCountryData[0] + ", " + SingleCountryData[8]), one);
            }
        }
    }
    

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
           
            Text key = t_key;
            int frequencyForCountry = 0;
            while (values.hasNext()) {
    
                IntWritable value = (IntWritable) values.next();
                
                // Contamos la cantidad de datos por cada llave (pais)
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
