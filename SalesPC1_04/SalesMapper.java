// CONSULTAR EL PROMEDIO DE VENTAS POR CADA PAÍS

package SalesCountry;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Para la suma total
    DoubleWritable venta;
    
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
            
            // Pasamos el valor de la columna precio
            venta = new DoubleWritable(Double.parseDouble(SingleCountryData[2]));

            // Nuestras llaves son los países
            output.collect(new Text(SingleCountryData[7]), venta);
        }
        
    }
}
