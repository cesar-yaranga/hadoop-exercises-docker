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
        String creacionCuenta = "";
        String ultimoLogueo = "";
        
        long msMaximo = 0;
        String nombreMaximo = "";
        String creacionCuentaMaximo = "";
        String ultimoLogueoMaximo = "";
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textNombre = (Text) value.getNombre();
            Text textCreacionCuenta = (Text) value.getCreacionCuenta();
            Text textUltimoLogueo = (Text) value.getUltimoLogueo();
            
            nombre = textNombre.toString();
            creacionCuenta = textCreacionCuenta.toString();
            ultimoLogueo = textUltimoLogueo.toString();
            
            try {  
                Date dateCreacion = new SimpleDateFormat("MM/dd/yy HH:mm").parse(creacionCuenta);
                Date dateUltimo = new SimpleDateFormat("MM/dd/yy HH:mm").parse(ultimoLogueo);
                        
                long ms =  dateUltimo.getTime() - dateCreacion.getTime();

                if (ms > msMaximo) {    
                    
                    msMaximo = ms;
                    nombreMaximo = nombre;
                    creacionCuentaMaximo = creacionCuenta;
                    ultimoLogueoMaximo = ultimoLogueo;
                }

            } catch (ParseException ex) {
                Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        output.collect(key, new CustomWritable(new Text(nombreMaximo) , new Text(creacionCuentaMaximo), new Text(ultimoLogueoMaximo)));
        
    }

    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        
        return Collections.max(list);
    }
}