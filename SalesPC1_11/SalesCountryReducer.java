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

        String nombre = "";
        String fechaTransaccion = "";
  
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textNombre = (Text) value.getNombre();
            Text textFechaTransaccion = (Text) value.getFechaTransaccion();

            
            nombre = textNombre.toString();
            fechaTransaccion = textFechaTransaccion.toString();
            
            String primeraLetra = nombre.substring(0, 1);
            
            if (primeraLetra.equals("E") || primeraLetra.equals("e")) { 
                
                output.collect(key, new CustomWritable(textNombre , textFechaTransaccion));
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