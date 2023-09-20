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

        private List<Double> itemPrices;

        public CustomWritable() {
            this.itemPrices = new ArrayList<>();
        }

        public void addItemPrice(double price) {
            itemPrices.add(price);
        }

        public List<Double> getItemPrices() {
            return itemPrices;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(itemPrices.size());
            for (Double price : itemPrices) {
                out.writeDouble(price);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            itemPrices.clear();
            for (int i = 0; i < size; i++) {
                itemPrices.add(in.readDouble());
            }
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
                // Validación para asegurarse de que itemPrice sea numérico
                double itemPrice;
                try {
                    itemPrice = Double.parseDouble(fields[4]);
                } catch (NumberFormatException e) {
                    // Manejar el caso en el que itemPrice no sea numérico
                    itemPrice = 0.0; // O asigna un valor predeterminado
                }

                CustomWritable customWritable = new CustomWritable();
                customWritable.addItemPrice(itemPrice);
                output.collect(new Text(itemType), customWritable);
            }
        }
    }

    public static class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Map<Double, Integer> priceCounts = new HashMap<>();

            while (values.hasNext()) {
                CustomWritable value = values.next();
                for (Double itemPrice : value.getItemPrices()) {
                    int count = priceCounts.getOrDefault(itemPrice, 0);
                    priceCounts.put(itemPrice, count + 1);
                }
            }

            double moda = 0;
            int maxCount = 0;
            for (Map.Entry<Double, Integer> entry : priceCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    moda = entry.getKey();
                }
            }

            String result = "Moda del precio: " + moda + ", Cantidad de veces que se repite: " + maxCount;
            output.collect(key, new Text(result));
        }
    }

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf = new JobConf(SalesCountryDriver.class);

        job_conf.setJobName("ModaCalculadora");
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
