import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ConnectedComponentsMapper extends Mapper<LongWritable, Text, Text, Text> {
    // input: u \t v  (edge)
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        String[] parts = line.split("\\s+");
        if (parts.length < 2) return;
        String u = parts[0];
        String v = parts[1];
        // Each node emits its neighbor and its own id as candidate label
        ctx.write(new Text(u), new Text(v));
        ctx.write(new Text(v), new Text(u));
    }
}
