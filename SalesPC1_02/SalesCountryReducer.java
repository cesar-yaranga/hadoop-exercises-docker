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
        String tarjeta = "";
        int sumaMastercard = 0;
        int sumaVisa = 0;
        int sumaDiners = 0;
        int sumaAmex = 0;
        int sumaTarjeta = 0;
        int sumamayor= 0;
        
        String tarjetaMayor = "";
        
        while (values.hasNext()) {

            CustomWritable value = (CustomWritable) values.next();
            
            IntWritable intWritableSumaTarjeta = (IntWritable) value.getSumaTarjeta();
            Text textTarjeta = (Text) value.getNombreTarjeta();
            
            tarjeta = textTarjeta.toString();
            sumaTarjeta = intWritableSumaTarjeta.get();
            
            if ("Mastercard".equals(tarjeta)) {sumaMastercard = sumaMastercard + sumaTarjeta; }
            if ("Visa".equals(tarjeta)) {sumaVisa = sumaVisa + sumaTarjeta; }
            if ("Diners".equals(tarjeta)) {sumaDiners = sumaDiners + sumaTarjeta;; }
            if ("Amex".equals(tarjeta)) {sumaAmex = sumaAmex + sumaTarjeta; }
            
        }
        
        int sumas[] = {sumaMastercard, sumaVisa, sumaDiners, sumaAmex};
        
        sumamayor = maximoValor(sumas);
        
            if (sumamayor == sumaMastercard) {tarjetaMayor = "Mastercard";}
            if (sumamayor == sumaVisa) {tarjetaMayor = "Visa";}
            if (sumamayor == sumaDiners) {tarjetaMayor = "Diners";}
            if (sumamayor == sumaAmex) {tarjetaMayor = "Amex";}
                                     
        output.collect(key, new CustomWritable(new Text(tarjetaMayor) ,  new IntWritable(sumamayor)));
        
    }
    
    public int maximoValor(int array[]){
        
        List<Integer> list = new ArrayList<Integer>();
        
        for (int i = 0; i < array.length; i++) {
          list.add(array[i]);
        }
        
        return Collections.max(list);
    }
    

}