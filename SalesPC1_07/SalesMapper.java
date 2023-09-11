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
    Text pais;
    Text estado;
    Text tarjeta;
    DoubleWritable monto;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
        
            pais = new Text(SingleCountryData[7]);
            estado = new Text(SingleCountryData[6]);
            tarjeta = new Text(SingleCountryData[3]);
            monto = new DoubleWritable(Integer.parseInt(SingleCountryData[2]));
 
            customWritable = new CustomWritable(pais, estado, tarjeta, monto);
            
            // Nuestras llaves son los productos
            output.collect(new Text(SingleCountryData[1]), customWritable);
        }
        
    }
}
