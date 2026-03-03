import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

public class EdgeRemovalReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        if (key.toString().equals("BET")) {
            // Collect betweenness into list and store to DistributedCache-like structure in memory
            List<Map.Entry<String, Double>> list = new ArrayList<>();
            for (Text t : values) {
                String[] parts = t.toString().split("\t");
                if (parts.length != 2) continue;
                String edge = parts[0];
                double b = Double.parseDouble(parts[1]);
                list.add(new AbstractMap.SimpleEntry<>(edge, b));
            }
            // sort descending by betweenness
            list.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            // decide top X% to remove
            Configuration conf = ctx.getConfiguration();
            double topPercent = conf.getDouble("girvan.removeTopPercent", 10.0);
            int toRemove = (int) Math.ceil(list.size() * topPercent / 100.0);
            Set<String> removeSet = new HashSet<>();
            for (int i = 0; i < toRemove && i < list.size(); i++) {
                removeSet.add(list.get(i).getKey());
            }

            // store removeSet temporarily into Job's configuration as a delimited string
            // (In real Hadoop you'd write to DistributedCache; here we'll write to context by emitting a special marker)
            for (String e : removeSet) {
                ctx.write(new Text("REMOVE_EDGE"), new Text(e));
            }
        } else if (key.toString().equals("GRAPH")) {
            // Graph edges will be processed in separate reduce step (we will collect REMOVE_EDGE lines first)
            // Here simply pass graph edges through tagged as "GRAPH"
            for (Text t : values) {
                ctx.write(new Text("GRAPH_EDGE"), t);
            }
        }
    }
}
