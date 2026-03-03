import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ModularityMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Input format: nodeId \t communityId \t neighbors
        String[] parts = value.toString().split("\t");
        if (parts.length < 3) return;

        String nodeId = parts[0];
        String community = parts[1];
        String[] neighbors = parts[2].split(",");

        // Emit edges inside community
        for (String neighbor : neighbors) {
            if (!neighbor.isEmpty()) {
                context.write(new Text(community), new Text(nodeId + "," + neighbor));
            }
        }
    }
}
