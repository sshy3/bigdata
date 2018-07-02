import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<Text,Text,Person,Text> {

    private Person person = new Person();
    public void map(Text key, Text val, Context context) throws IOException, InterruptedException {
        person.setFirstname(key);
        person.setLastname(val);
        context.write(person,val);
    }
}
