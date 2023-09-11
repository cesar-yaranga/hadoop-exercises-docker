package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

    CustomWritable customWritable;
    Text textCreacionCuenta;
    Text textNombre;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
        
            textCreacionCuenta = new Text(SingleCountryData[8]);
            textNombre = new Text(SingleCountryData[4]);
 
            customWritable = new CustomWritable(textNombre, textCreacionCuenta);
            
            // Nuestras llaves son los países
            output.collect(new Text(SingleCountryData[7]), customWritable);
        }
        
    }
}
