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

    private Text producto;



    public CustomWritable() {
        producto = new Text();
    }

    public CustomWritable(Text producto) {
        this.producto = producto;
    }

    public Text getProducto() {
        return producto;
    }

    public void setProducto(Text producto) {
        this.producto = producto;
    }



    public void readFields(DataInput in) throws IOException {
        producto.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        producto.write(out);
    }

    @Override
    public String toString() {
        return producto.toString();
    }
}