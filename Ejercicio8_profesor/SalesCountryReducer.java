package SalesCountry;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        //int frequencyForCountry = 0;
        int menor_tipo = Integer.MAX_VALUE;//int mayor_tipo = Integer.MIN_VALUE;
        while (values.hasNext()) {            
            IntWritable value = (IntWritable) values.next();
            if(value.get() < menor_tipo){//if(value.get() > mayor_tipo){
                menor_tipo = value.get();                        
            }
        }
        output.collect(key, new IntWritable(menor_tipo));
    }
}
