import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ModularityReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Key = communityId
        // Values = list of node pairs in this community
        int internalEdges = 0;
        int totalEdges = 0;

        for (Text val : values) {
            totalEdges++;
            // For now, assume each edge inside same community is internal
            internalEdges++;
        }

        // Simplified modularity measure
        double modularity = 0.0;
        if (totalEdges > 0) {
            modularity = (double) internalEdges / totalEdges;
        }

        context.write(key, new DoubleWritable(modularity));
    }
}
