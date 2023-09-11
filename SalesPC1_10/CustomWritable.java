package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Text;

public class CustomWritable implements Writable {

    private Text nombre;
    private Text creacionCuenta;



    public CustomWritable() {
        nombre = new Text();
        creacionCuenta = new Text();


    }

    public CustomWritable(Text nombre, Text creacionCuenta) {
        this.nombre = nombre;
        this.creacionCuenta = creacionCuenta;
 
    }

    public Text getNombre() {
        return nombre;
    }

    public void setNombre(Text nombre) {
        this.nombre = nombre;
    }

    public Text getCreacionCuenta() {
        return creacionCuenta;
    }

    public void setCreacionCuenta(Text creacionCuenta) {
        this.creacionCuenta = creacionCuenta;
    }


    public void readFields(DataInput in) throws IOException {
        nombre.readFields(in);
        creacionCuenta.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        nombre.write(out);
        creacionCuenta.write(out);
    }

    @Override
    public String toString() {
        return nombre.toString() + "\t" + creacionCuenta.toString() + "\t";
    }
}