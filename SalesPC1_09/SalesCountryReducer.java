package SalesCountry;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapred.*;
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, Text> {

    @Override
    public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       
        Text key = t_key;
        int contadorP1 = 0;
        int contadorP2 = 0;
        int contadorP3 = 0;
        String producto;
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textProducto = (Text) value.getProducto();
            producto = textProducto.toString();
            
            if (producto.equals("Product1")) {
                contadorP1++;
            }
            
            if (producto.equals("Product2")) {
                contadorP2++;
            }
            
            if (producto.equals("Product3")) {
                contadorP3++;
            }
        }
        
        int total = contadorP1 + contadorP2 + contadorP3;
        
        double porcentajeP1 = ((double) contadorP1 / total)*100;
        double porcentajeP2 = ((double) contadorP2 / total)*100;
        double porcentajeP3 = ((double) contadorP3 / total)*100;
            
        output.collect(key, new Text(Double.toString(Math.round(porcentajeP1*100.0)/100.0) + " " + 
                Double.toString(Math.round(porcentajeP2*100.0)/100.0) + " " + Double.toString(Math.round(porcentajeP3*100.0)/100.0)));


        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}