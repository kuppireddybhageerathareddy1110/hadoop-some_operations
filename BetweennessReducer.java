import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import java.util.*;

public class BetweennessReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        // Build adjacency
        Map<String, List<String>> graph = new HashMap<>();
        for (Text t : values) {
            String[] uv = t.toString().split(",");
            if (uv.length != 2) continue;
            String u = uv[0], v = uv[1];
            graph.computeIfAbsent(u, k -> new ArrayList<>()).add(v);
        }

        // Run Brandes' algorithm for all nodes
        Map<String, Double> edgeBet = new HashMap<>(); // edgeKey -> betweenness (double)

        for (String s : graph.keySet()) {
            // Single-source shortest-paths BFS
            Stack<String> stack = new Stack<>();
            Map<String, List<String>> pred = new HashMap<>();
            Map<String, Integer> dist = new HashMap<>();
            Map<String, Double> sigma = new HashMap<>();

            for (String v : graph.keySet()) {
                pred.put(v, new ArrayList<>());
                dist.put(v, -1);
                sigma.put(v, 0.0);
            }

            dist.put(s, 0);
            sigma.put(s, 1.0);
            Queue<String> q = new LinkedList<>();
            q.add(s);

            while (!q.isEmpty()) {
                String v = q.poll();
                stack.push(v);
                for (String w : graph.getOrDefault(v, Collections.emptyList())) {
                    if (dist.get(w) < 0) {
                        dist.put(w, dist.get(v) + 1);
                        q.add(w);
                    }
                    if (dist.get(w) == dist.get(v) + 1) {
                        sigma.put(w, sigma.get(w) + sigma.get(v));
                        pred.get(w).add(v);
                    }
                }
            }

            Map<String, Double> delta = new HashMap<>();
            for (String v : graph.keySet()) delta.put(v, 0.0);

            while (!stack.isEmpty()) {
                String w = stack.pop();
                for (String v : pred.get(w)) {
                    double c = (sigma.get(v) / sigma.get(w)) * (1.0 + delta.get(w));
                    delta.put(v, delta.get(v) + c);
                    String edgeKey = getEdgeKey(v, w);
                    edgeBet.put(edgeKey, edgeBet.getOrDefault(edgeKey, 0.0) + c);
                }
                if (!w.equals(s)) {
                    // nothing else
                }
            }
        }

        // For undirected graphs Brandes yields edge betweenness counts that should be divided by 2
        for (Map.Entry<String, Double> e : edgeBet.entrySet()) {
            double val = e.getValue() / 2.0;
            ctx.write(new Text(e.getKey()), new DoubleWritable(val));
        }
    }

    private String getEdgeKey(String a, String b) {
        return (a.compareTo(b) < 0) ? a + "-" + b : b + "-" + a;
    }
}
