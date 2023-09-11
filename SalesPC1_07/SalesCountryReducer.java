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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.*;
public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, CustomWritable, Text, CustomWritable> {

    @Override
    public void reduce(Text t_key, Iterator<CustomWritable> values, OutputCollector<Text, CustomWritable> output, Reporter reporter) throws IOException {
       
        Text key = t_key;

        String pais = "";
        String estado = "";
        String tarjeta = "";
        double monto = 0;
        int cont = 0;
        double promedio;
        double montoAcumulado = 0;
                
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();

            Text textPais = (Text) value.getPais();
            Text textEstado = (Text) value.getEstado();
            Text textTarjeta = (Text) value.getTarjeta();
            DoubleWritable DWMonto = (DoubleWritable) value.getMonto();
            
            pais = textPais.toString();
            estado = textEstado.toString();
            tarjeta = textTarjeta.toString();
            monto = DWMonto.get();
            
            if (pais.equals("United Kingdom") && estado.equals("England") && tarjeta.equals("Visa")) {
            
            montoAcumulado = montoAcumulado + monto;
            cont++;
            
            }
        }
        
        promedio = (double) montoAcumulado / cont;
        output.collect(key, new CustomWritable(new Text("United Kingdom") , new Text("England"), new Text("Visa"), new DoubleWritable (Math.round(promedio*100.0)/100.0)));
        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}