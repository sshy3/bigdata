import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class HadoopUtility {

    public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fs= FileSystem.get(conf);
        Path path=fs.getHomeDirectory();
        System.out.println(path.getName());

    }
}
