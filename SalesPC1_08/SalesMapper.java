package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, CustomWritable> {

    CustomWritable customWritable;
    Text textEstado;
    DoubleWritable DWLongitud;
    DoubleWritable DWLatitud;
    DoubleWritable DWMonto;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
        
            textEstado = new Text(SingleCountryData[6]);
            DWLongitud = new DoubleWritable(Double.parseDouble(SingleCountryData[10]));
            DWLatitud = new DoubleWritable(Double.parseDouble(SingleCountryData[11]));
            DWMonto = new DoubleWritable(Double.parseDouble(SingleCountryData[2]));

 
            customWritable = new CustomWritable(textEstado, DWLongitud, DWLatitud, DWMonto);
            
            // Nuestras llaves son los países
            output.collect(new Text(SingleCountryData[7]), customWritable);
        }
        
    }
}
