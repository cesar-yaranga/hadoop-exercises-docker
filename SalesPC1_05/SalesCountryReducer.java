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
        String nombre = "";
        String fechaHora = "";
        
        String fechaMayor = "1/1/09 0:0";
        String ciudadMayor = "";
        String nombreMayor = "";
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();
            
            Text textCiudad = (Text) value.getCiudad();
            Text textNombre = (Text) value.getNombre();
            Text textFechaHora = (Text) value.getFechaHora();
            
            ciudad = textCiudad.toString();
            nombre = textNombre.toString();
            fechaHora = textFechaHora.toString();
            
            try {  
                Date date = new SimpleDateFormat("MM/dd/yy HH:mm").parse(fechaHora);
                Date dateMayor = new SimpleDateFormat("MM/dd/yy HH:mm").parse(fechaMayor);
                
                if (date.compareTo(dateMayor) > 0) {                  
                    fechaMayor = fechaHora;
                    ciudadMayor = ciudad;
                    nombreMayor = nombre;
                }

            } catch (ParseException ex) {
                Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        output.collect(key, new CustomWritable(new Text(ciudadMayor) , new Text(nombreMayor), new Text(fechaMayor)));
        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}