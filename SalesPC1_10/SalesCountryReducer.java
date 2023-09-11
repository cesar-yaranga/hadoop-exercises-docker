package SalesCountry;
import java.io.IOException;
import java.text.DateFormat;
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
        String creacionCuenta = "";
                
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textNombre = (Text) value.getNombre();
            Text textCreacionCuenta = (Text) value.getCreacionCuenta();
            
            nombre = textNombre.toString();
            creacionCuenta = textCreacionCuenta.toString();
            
            try {  
              
                DateFormat df = new SimpleDateFormat("MM/dd/yy HH:mm");
                Calendar cal  = Calendar.getInstance();
                cal.setTime(df.parse(creacionCuenta));
                
                int dia = cal.get(Calendar.DAY_OF_MONTH);
                int mes = cal.get(Calendar.MONTH);
                int hora = cal.get(Calendar.HOUR_OF_DAY);
                int minutos = cal.get(Calendar.MINUTE);
                
                if (dia == 5 && mes == 0 && hora == 2 && minutos == 23) {
                    
                    output.collect(key, new CustomWritable(textNombre , textCreacionCuenta));
                                    
                }

            } catch (ParseException ex) {
                Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
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