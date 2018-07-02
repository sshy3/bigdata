import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.security.auth.login.Configuration;
import java.io.IOException;

public class AvgWordLengthReducer extends Reducer<Text,Iterable<IntWritable>,Text,DoubleWritable> {
    public void Reduce(Text key, Iterable<IntWritable> list, Context context) throws IOException, InterruptedException {
        int sum=0;
        int n=0;
        for(IntWritable value:list){
            sum=sum+value.get();
            n=n+1;
        }
        double avglen=(1.0d*sum)/n;
        System.out.println("sree:"+avglen);
        DoubleWritable finalvalue=new DoubleWritable(avglen);
        context.write(key,finalvalue);
    }
}
