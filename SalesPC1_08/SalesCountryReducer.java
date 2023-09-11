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
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, CustomWritable> {

    @Override
    public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
       
        Text key = t_key;

        String estado = "";
        double lon = 0;
        double lat = 0;
        double monto;
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textEstado = (Text) value.getEstado();
            DoubleWritable DWLon = (DoubleWritable) value.getLongitud();
            DoubleWritable DWLat = (DoubleWritable) value.getLatitud();
            DoubleWritable DWMonto = (DoubleWritable) value.getMonto();
            
            estado = textEstado.toString();
            lon = DWLon.get();
            lat = DWLat.get();
            monto = DWMonto.get();
            
            if (t_key.toString().equals("United States")) {
            output.collect(key, new CustomWritable(new Text(estado) , new DoubleWritable(lon) , new DoubleWritable(lat), new DoubleWritable(monto)));

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