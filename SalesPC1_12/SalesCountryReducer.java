package SalesCountry;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapred.*;
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, CustomWritable> {

    @Override
    public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
       
        Text key = t_key;

        String ciudad = "";
        String fechaTransaccion = "";
  
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textCiudad = (Text) value.getCiudad();
            Text textFechaTransaccion = (Text) value.getFechaTransaccion();
            
            ciudad = textCiudad.toString();
            fechaTransaccion = textFechaTransaccion.toString();
            
            String[] split1 = fechaTransaccion.split(" ");
            String[] split2 = split1[1].split(":");
            
            int hora = Integer.parseInt(split2[0]);
            
            if (hora >= 0 && hora <= 4 ) { 
                
                output.collect(key, new CustomWritable(textCiudad , textFechaTransaccion));
            }
            

        }
        
        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}