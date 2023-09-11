// CONSULTAR EL PROMEDIO DE VENTAS POR CADA PAÍS

package SalesCountry;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
       
        Text key = t_key;
        double sumaPais = 0;
        int contadorPais = 0;
        double promedioPais; 
        
        while (values.hasNext()) {
            
            DoubleWritable value = (DoubleWritable) values.next();
            
            // Sumamos y contamos
            sumaPais += value.get();
            contadorPais++;
        }
        
        promedioPais = (double) sumaPais / contadorPais;
                
        output.collect(key, new DoubleWritable (Math.round(promedioPais*100.0)/100.0));
    }
}
