import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommunityMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) return;

        // Accept tab-separated: nodeId \t label \t neighbor1,neighbor2,...
        String[] parts = line.split("\t");
        if (parts.length < 2) {
            // Fallback: whitespace split
            parts = line.split("\\s+");
        }
        if (parts.length < 2) return;

        String nodeId = parts[0].trim();
        String currentLabel = parts[1].trim();
        String neighbors = parts.length >= 3 ? parts[2].trim() : "";

        // Emit SELF message with neighbors preserved
        context.write(new Text(nodeId), new Text("SELF#" + currentLabel + "#" + neighbors));

        // Emit VOTE messages to neighbors
        if (!neighbors.isEmpty()) {
            String[] nbrs = neighbors.split(",");
            for (String n : nbrs) {
                n = n.trim();
                if (!n.isEmpty()) {
                    context.write(new Text(n), new Text("VOTE#" + currentLabel));
                }
            }
        }
    }
}
