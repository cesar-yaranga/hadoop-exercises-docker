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
    private Text fechaTransaccion;



    public CustomWritable() {
        nombre = new Text();
        fechaTransaccion = new Text();

    }

    public CustomWritable(Text nombre, Text fechaTransaccion) {
        this.nombre = nombre;
        this.fechaTransaccion = fechaTransaccion;


    }

    public Text getNombre() {
        return nombre;
    }

    public void setNombre(Text nombre) {
        this.nombre = nombre;
    }

    public Text getFechaTransaccion() {
        return fechaTransaccion;
    }

    public void setFechaTransaccion(Text fechaTransaccion) {
        this.fechaTransaccion = fechaTransaccion;
    }

    public void readFields(DataInput in) throws IOException {
        nombre.readFields(in);
        fechaTransaccion.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        nombre.write(out);
        fechaTransaccion.write(out);
    }

    @Override
    public String toString() {
        return nombre.toString() + "\t" + fechaTransaccion.toString() + "\t";
    }
}