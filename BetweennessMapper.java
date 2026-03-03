import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class BetweennessMapper extends Mapper<LongWritable, Text, Text, Text> {
    // Input: "u \t v"
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) return;
        String[] parts = line.split("\\s+");
        if (parts.length < 2) return;
        String u = parts[0];
        String v = parts[1];

        // emit neighbor info
        ctx.write(new Text("NODE"), new Text(u + "," + v));
        ctx.write(new Text("NODE"), new Text(v + "," + u));
    }
}
