import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class EmployeeMapper extends Mapper<LongWritable,Text,Text,Text> {
    private HashMap<String,String> locationtable = new HashMap<>();
    private URI[] files;
    public void map(LongWritable keyin, Text val, Context context) throws IOException, InterruptedException {
    String splits[] = val.toString().split(" ");
    if(locationtable.containsKey(splits[6])){
        context.write(new Text(splits[0]+":"+splits[6]), new Text(locationtable.get(splits[6])));

    }else{
        context.write(new Text(splits[0]+":"+splits[6]), new Text("No Location Found"));
    }

    }
    public void setup(Context context) throws IOException {
        files=context.getCacheFiles();
        System.out.println(files);
        Path path = new Path(files[0]);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line ="";
        while ((line=br.readLine())!=null)
        {String splits[] = line.split(",");
        locationtable.put(splits[0],splits[1]);

        }
        br.close();
        in.close();
    }
}
