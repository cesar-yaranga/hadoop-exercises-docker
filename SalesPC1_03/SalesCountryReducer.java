package SalesCountry;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, CustomWritable> {

        @Override
    public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
       
        Text key = t_key;
        String nombrePersona = "";
        int cantidadDinero = 0;

        int cantidadDineroMayor = 0;        
        String nombrePersonaMayor = "";
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();
            
            IntWritable intWritableCantidadDinero = (IntWritable) value.getCantidadDinero();
            Text textNombrePersona = (Text) value.getNombrePersona();
            
            nombrePersona = textNombrePersona.toString();
            cantidadDinero = intWritableCantidadDinero.get();
            
            if (cantidadDinero >= cantidadDineroMayor) {
            
                cantidadDineroMayor = cantidadDinero;
                nombrePersonaMayor = nombrePersona;
            }
        }
                 
        output.collect(key, new CustomWritable(new Text(nombrePersonaMayor) ,  new IntWritable(cantidadDineroMayor)));
        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}