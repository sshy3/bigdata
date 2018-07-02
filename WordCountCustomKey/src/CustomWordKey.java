import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWordKey implements WritableComparable<CustomWordKey> {
    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    private Text word;
    public CustomWordKey(){
        word = new Text();
    }
    @Override
    public int compareTo(CustomWordKey o) {
        return this.word.compareTo(o.getWord());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
    }
}
