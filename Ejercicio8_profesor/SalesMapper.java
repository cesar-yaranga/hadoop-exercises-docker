package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    //private final static IntWritable one = new IntWritable(1);
    IntWritable numero;
    Text txttotal = new Text("cteSumaTotal");
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        if("Product1".equals(SingleCountryData[1])){
            numero = new IntWritable(Integer.parseInt(SingleCountryData[2]));
            output.collect(txttotal, numero);
        }else{
            //numero = new IntWritable(0);
        }
        //output.collect(txttotal, numero);
        //output.collect(new Text(SingleCountryData[1] + SingleCountryData[3]), numero);
    }
}
