package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Text Array Writable
 */
public class TextArrayWritable extends ArrayWritable{
    public TextArrayWritable() { super(Text.class);}

    public Text[] get() {
        final Writable[] writables = super.get();
        Text[] texts = new Text[writables.length];
        for (int i = 0; i < writables.length; i++) {
            texts[i] = (Text)writables[i];
        }
        return texts;
    }

    public void set(final Text... values) {
        Text[] texts = new Text[values.length];
        System.arraycopy(values, 0, texts, 0, values.length);
        super.set(texts);
    }

    public String[] getStrings() {
        Text[] texts = get();
        String[] strings = new String[texts.length];
        for (int i = 0; i < texts.length; i++) {
            strings[i] = texts[i].toString();
        }
        return strings;
    }

    @Override
    public String toString() {
        return Arrays.toString(getStrings());
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof TextArrayWritable) {
            TextArrayWritable otaw = (TextArrayWritable) other;
            if (this.get().length != otaw.get().length) return false;

            for (int i = 0; i < this.get().length; i++) {
                if (!this.get()[i].equals(otaw.get()[i])) return false;
            }
            return true;
        } else return false;
    }
}
