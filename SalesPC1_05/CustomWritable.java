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
    private Text nombre;
    private Text fechaHora;



    public CustomWritable() {
        ciudad = new Text();
        nombre = new Text();
        fechaHora = new Text();


    }

    public CustomWritable(Text ciudad, Text nombre, Text fechaHora) {
        this.ciudad = ciudad;
        this.nombre = nombre;
        this.fechaHora = fechaHora;

    }

    public Text getCiudad() {
        return ciudad;
    }

    public void setCiudad(Text ciudad) {
        this.ciudad = ciudad;
    }

    public Text getNombre() {
        return nombre;
    }

    public void setNombre(Text nombre) {
        this.nombre = nombre;
    }

    public Text getFechaHora() {
        return fechaHora;
    }

    public void setFechaHora(Text fechaHora) {
        this.fechaHora = fechaHora;
    }


    public void readFields(DataInput in) throws IOException {
        ciudad.readFields(in);
        nombre.readFields(in);
        fechaHora.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        ciudad.write(out);
        nombre.write(out);
        fechaHora.write(out);
    }

    @Override
    public String toString() {
        return ciudad.toString() + "\t" + nombre.toString() + "\t" + fechaHora.toString();
    }
}