import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomPartitionerReducer extends Reducer<Text,Text,Text, NullWritable> {
    public void reducer(Text opval,Iterable<Text> listval,Context context) throws IOException, InterruptedException {

        for(Text word:listval) {
            context.write(word,null);
        }
    }
}
