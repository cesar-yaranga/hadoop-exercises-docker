package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

    CustomWritable customWritable;
    Text fechaTransaccion;
    Text textNombre;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
        
            fechaTransaccion = new Text(SingleCountryData[0]);
            textNombre = new Text(SingleCountryData[4]);
 
            customWritable = new CustomWritable(textNombre, fechaTransaccion);
            
            // Nuestras llaves son las tarjetas
            output.collect(new Text(SingleCountryData[3]), customWritable);
        }
        
    }
}
