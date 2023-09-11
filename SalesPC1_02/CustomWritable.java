package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class CustomWritable implements Writable {

    private Text nombreTarjeta;
    private IntWritable sumaTarjeta;


    public CustomWritable() {
        nombreTarjeta = new Text();
        sumaTarjeta = new IntWritable(0);

    }

    public CustomWritable(Text nombreTarjeta, IntWritable sumaTarjeta) {
        this.nombreTarjeta = nombreTarjeta;
        this.sumaTarjeta = sumaTarjeta;

    }

    public Text getNombreTarjeta() {
        return nombreTarjeta;
    }

    public IntWritable getSumaTarjeta() {
        return sumaTarjeta;
    }

    public void setNombreTarjeta(Text nombreTarjeta) {
        this.nombreTarjeta = nombreTarjeta;
    }

    public void setSumaTarjeta(IntWritable sumaTarjeta) {
        this.sumaTarjeta = sumaTarjeta;
    }

    public void readFields(DataInput in) throws IOException {
        nombreTarjeta.readFields(in);
        sumaTarjeta.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        nombreTarjeta.write(out);
        sumaTarjeta.write(out);
    }

    @Override
    public String toString() {
        return nombreTarjeta.toString() + "\t" + sumaTarjeta.toString();
    }

}