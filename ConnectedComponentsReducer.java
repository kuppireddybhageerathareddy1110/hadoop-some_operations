import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ConnectedComponentsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        String minId = key.toString();
        Set<String> neigh = new HashSet<>();
        for (Text t : values) {
            String v = t.toString();
            neigh.add(v);
            if (v.compareTo(minId) < 0) minId = v;
        }
        // Emit node -> communityMin
        ctx.write(key, new Text(minId));
        // Optionally emit adjacency back to file for next iteration (some CC implementations require adjacency iteration)
    }
}
