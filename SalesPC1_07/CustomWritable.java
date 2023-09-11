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
    
    private Text pais;
    private Text estado;
    private Text tarjeta;
    private DoubleWritable monto;


    public CustomWritable() {
        pais = new Text();
        estado = new Text();
        tarjeta = new Text();
        monto = new DoubleWritable();


    }

    public CustomWritable(Text pais, Text estado, Text tarjeta, DoubleWritable monto) {
        this.pais = pais;
        this.estado = estado;
        this.tarjeta = tarjeta;
        this.monto = monto;

    }

    public Text getPais() {
        return pais;
    }

    public void setPais(Text pais) {
        this.pais = pais;
    }

    public Text getEstado() {
        return estado;
    }

    public void setEstado(Text estado) {
        this.estado = estado;
    }

    public Text getTarjeta() {
        return tarjeta;
    }

    public void setTarjeta(Text tarjeta) {
        this.tarjeta = tarjeta;
    }

    public DoubleWritable getMonto() {
        return monto;
    }

    public void setMonto(DoubleWritable monto) {
        this.monto = monto;
    }


 
    public void readFields(DataInput in) throws IOException {
        pais.readFields(in);
        estado.readFields(in);
        tarjeta.readFields(in);
        monto.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        pais.write(out);
        estado.write(out);
        tarjeta.write(out);
        monto.write(out);
    }

    @Override
    public String toString() {
        return pais.toString() + "\t" + estado.toString() + "\t" + tarjeta.toString()  + "\t" + monto.toString();
    }
}