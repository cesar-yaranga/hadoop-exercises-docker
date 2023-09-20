/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package SalesCountryDriver;

/**
 *
 * @author Desktop
 */
// package SalesCountry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {

        private String field1;
        private String field2;
        private double field3;

        public CustomWritable() {
            // Constructor sin argumentos requerido por Hadoop
        }

        public CustomWritable(String field1, String field2, double field3) {
            this.field1 = field1;
            this.field2 = field2;
            this.field3 = field3;
        }

        public String getField1() {
            return field1;
        }

        public String getField2() {
            return field2;
        }

        public double getField3() {
            return field3;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // Escribe los campos en el flujo de datos
            out.writeUTF(field1);
            out.writeUTF(field2);
            out.writeDouble(field3);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // Lee los campos desde el flujo de datos
            field1 = in.readUTF();
            field2 = in.readUTF();
            field3 = in.readDouble();
        }

        @Override
        public String toString() {
            // Devuelve una representación de cadena de los campos
            return field2 + "\t" + field3;
        }

        // Agrega getters y setters según sea necesario
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            String valueString = value.toString();
            String[] fields = valueString.split(",");

            // Realiza la lógica de tu consulta aquí, por ejemplo, buscar subcadenas en el campo item_description
            String itemDescription = fields[5];
            if (itemDescription.contains("White")) {
                String field1 = fields[3];
                String field2 = fields[5];
                double field3 = Double.parseDouble(fields[4]);
                CustomWritable customWritable = new CustomWritable(field1, field2, field3);
                output.collect(new Text(field1), customWritable);
            }
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // Aquí puedes realizar cualquier operación adicional en los datos que coinciden con tu consulta
            while (values.hasNext()) {
                CustomWritable value = values.next();
                output.collect(key, new Text(value.toString()));
            }
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
