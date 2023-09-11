package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class CustomWritable implements Writable {

    private Text nombrePersona;
    private IntWritable cantidadDinero;


    public CustomWritable() {
        nombrePersona = new Text();
        cantidadDinero = new IntWritable(0);

    }

    public CustomWritable(Text nombrePersona, IntWritable cantidadDinero) {
        this.nombrePersona = nombrePersona;
        this.cantidadDinero = cantidadDinero;

    }

    public Text getNombrePersona() {
        return nombrePersona;
    }

    public void setNombrePersona(Text nombrePersona) {
        this.nombrePersona = nombrePersona;
    }

    public IntWritable getCantidadDinero() {
        return cantidadDinero;
    }

    public void setCantidadDinero(IntWritable cantidadDinero) {
        this.cantidadDinero = cantidadDinero;
    }


    public void readFields(DataInput in) throws IOException {
        nombrePersona.readFields(in);
        cantidadDinero.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        nombrePersona.write(out);
        cantidadDinero.write(out);
    }

    @Override
    public String toString() {
        return nombrePersona.toString() + "\t" + cantidadDinero.toString();
    }

}