//package SalesCountryDriver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

public class SalesCountryDriver {

    public static class CustomWritable implements Writable {

        private String estado;
        private double precio;

        public CustomWritable() {
            // Constructor sin argumentos requerido por Hadoop
            this.estado = "";
            this.precio = 0;
        }

        public CustomWritable(String estado, Double precio) {
            this.estado = estado;
            this.precio = precio;
        }

        public String getEstado() {
            return estado;
        }

        public double getPrecio() {
            return precio;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(estado);
            out.writeDouble(precio);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            estado = in.readUTF();
            precio = in.readDouble();
        }
    }

    public static class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
            String valueString = value.toString();
            String[] fields = valueString.split(",");

            if (!"Price".equals(fields[2])) {
                String pais = fields[7];
                String estado = fields[6];
                double precio = Double.parseDouble(fields[2]);

                CustomWritable customWritable = new CustomWritable(estado, precio);

                output.collect(new Text(pais), customWritable);
            }
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String estado = "";
            
            double total = 0;
            double cuarta = 0;
            double precio = 0;

            List<CustomWritable> customWritableList = new ArrayList<>();

            while (values.hasNext()) {
                CustomWritable customWritable = values.next();
                customWritableList.add(customWritable);
            }

            for (CustomWritable value : customWritableList) {
                double currentPrecio = value.getPrecio();

                total += currentPrecio;
            }    

            cuarta = total / 4;

            for (CustomWritable value : customWritableList) {
                String currentEstado = value.getEstado();
                double currentPrecio = value.getPrecio();

                if (currentPrecio >= cuarta) {
                    String result = currentEstado + "\t" + Double.toString(currentPrecio);
                    output.collect(key, new Text(result));       
                }
            }
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
