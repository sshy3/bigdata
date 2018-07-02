import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TestChild;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<Person,Text, Text, Text> {
    public void reduce(Person key, Iterable<Text> vallist, Context context) throws IOException, InterruptedException {

        for(Text curval:vallist){

            context.write(key.getFirstname(),curval);
        }

    }
}
