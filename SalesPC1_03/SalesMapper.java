package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

    // Para las sumas y nombres de tarjetas
    CustomWritable customWritable;
    Text textNombrePersona;
    IntWritable intWritableCantidadDinero;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
        
            textNombrePersona = new Text(SingleCountryData[4]);
            intWritableCantidadDinero = new IntWritable(Integer.parseInt(SingleCountryData[2]));
 
            customWritable = new CustomWritable(textNombrePersona, intWritableCantidadDinero);
            
            // Nuestras llaves son los países
            output.collect(new Text(SingleCountryData[7]), customWritable);
        }
        
    }
}
