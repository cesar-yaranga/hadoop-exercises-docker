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
    Text textProducto;
    Text textPais;
    Text textEstado;
    Text textCiudad;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
               
        // Para no utilizar los encabezados
        if(!"Price".equals(SingleCountryData[2])) {
            
            textProducto = new Text(SingleCountryData[1]);
            textPais = new Text(SingleCountryData[7]);
            textEstado = new Text(SingleCountryData[6]);
            textCiudad = new Text(SingleCountryData[5]);


            customWritable = new CustomWritable(textProducto);
            
            // Nuestras llaves son compuestas
            output.collect(new Text(textPais + "," + textEstado + "," + textCiudad), customWritable);
        }
        
    }
}
