import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EdgeRemovalMapper extends Mapper<LongWritable, Text, Text, Text> {
    // We'll emit all lines as-is keyed by "DATA"
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        // tag input lines: betweenness lines have "-" in key (edge), graph lines are two tokens
        if (line.contains("\t")) {
            String[] parts = line.split("\t");
            if (parts.length == 2 && parts[0].contains("-")) {
                // betweenness line emitted by betweenness reducer
                ctx.write(new Text("BET"), new Text(parts[0] + "\t" + parts[1]));
                return;
            }
        }
        // otherwise treat as graph edge "u v" or "u\tv"
        ctx.write(new Text("GRAPH"), new Text(line));
    }
}
