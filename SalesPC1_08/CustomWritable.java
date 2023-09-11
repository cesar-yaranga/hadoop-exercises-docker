package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class CustomWritable implements Writable {

    private Text estado;
    private DoubleWritable longitud;
    private DoubleWritable latitud;
    private DoubleWritable monto;




    public CustomWritable() {
        estado = new Text();
        longitud = new DoubleWritable();
        latitud = new DoubleWritable();
        monto = new DoubleWritable();


    }

    public CustomWritable(Text estado, DoubleWritable longitud, DoubleWritable latitud, DoubleWritable monto) {
        this.estado = estado;
        this.longitud = longitud;
        this.latitud = latitud;
        this.monto = monto;
    }

    public Text getEstado() {
        return estado;
    }

    public void setEstado(Text estado) {
        this.estado = estado;
    }

    public DoubleWritable getLongitud() {
        return longitud;
    }

    public void setLongitud(DoubleWritable longitud) {
        this.longitud = longitud;
    }

    public DoubleWritable getLatitud() {
        return latitud;
    }

    public void setLatitud(DoubleWritable latitud) {
        this.latitud = latitud;
    }

    public DoubleWritable getMonto() {
        return monto;
    }

    public void setMonto(DoubleWritable monto) {
        this.monto = monto;
    }



    public void readFields(DataInput in) throws IOException {
        estado.readFields(in);
        longitud.readFields(in);
        latitud.readFields(in);
        monto.readFields(in);

    }

    public void write(DataOutput out) throws IOException {
        estado.write(out);
        longitud.write(out);
        latitud.write(out);
        monto.write(out);
    }

    @Override
    public String toString() {
        return estado.toString() + "\t" + longitud.toString() + "\t" + latitud.toString() + "\t" + monto.toString() + "\t";
    }
}