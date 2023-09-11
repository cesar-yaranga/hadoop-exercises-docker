package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Text;

public class CustomWritable implements Writable {

    private Text ciudad;
    private Text fechaTransaccion;



    public CustomWritable() {
        ciudad = new Text();
        fechaTransaccion = new Text();

    }

    public CustomWritable(Text ciudad, Text fechaTransaccion) {
        this.ciudad = ciudad;
        this.fechaTransaccion = fechaTransaccion;


    }

    public Text getCiudad() {
        return ciudad;
    }

    public void setCiudad(Text ciudad) {
        this.ciudad = ciudad;
    }

    public Text getFechaTransaccion() {
        return fechaTransaccion;
    }

    public void setFechaTransaccion(Text fechaTransaccion) {
        this.fechaTransaccion = fechaTransaccion;
    }


    public void readFields(DataInput in) throws IOException {
        ciudad.readFields(in);
        fechaTransaccion.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        ciudad.write(out);
        fechaTransaccion.write(out);
    }

    @Override
    public String toString() {
        return ciudad.toString() + "\t" + fechaTransaccion.toString() + "\t";
    }
}