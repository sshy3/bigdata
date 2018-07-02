import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SearchWordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text keyin,Iterable<IntWritable> vallist,Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable val:vallist){
            sum=sum+val.get();
        }
        context.write(keyin,new IntWritable(sum));
    }
}
