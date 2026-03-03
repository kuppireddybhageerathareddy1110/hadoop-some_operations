import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommunityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String currentLabel = null;
        String neighbors = "";
        Map<String, Integer> labelCounts = new HashMap<>();

        for (Text v : values) {
            String s = v.toString();
            String[] parts = s.split("#", 3); // type#label[#neighbors]
            if (parts.length >= 1 && "SELF".equals(parts[0])) {
                currentLabel = parts.length > 1 ? parts[1] : "";
                if (parts.length > 2) neighbors = parts[2];
            } else if (parts.length >= 2 && "VOTE".equals(parts[0])) {
                String label = parts[1];
                labelCounts.put(label, labelCounts.getOrDefault(label, 0) + 1);
            }
        }

        if (currentLabel == null) {
            // Some nodes might only receive votes but had no SELF (defensive)
            currentLabel = key.toString();
        }

        // Select label with max votes; tie-breaker: numeric smaller if numeric else lexicographic
        String newLabel = currentLabel;
        int maxCount = -1;
        for (Map.Entry<String, Integer> e : labelCounts.entrySet()) {
            String lbl = e.getKey();
            int cnt = e.getValue();
            if (cnt > maxCount) {
                maxCount = cnt;
                newLabel = lbl;
            } else if (cnt == maxCount && cnt != -1) {
                if (tieBreak(lbl, newLabel) < 0) {
                    newLabel = lbl;
                }
            }
        }

        // If no votes, keep current label
        if (maxCount <= 0) {
            newLabel = currentLabel;
        }

        // If label changed, increment counter for convergence checking
        if (!newLabel.equals(currentLabel)) {
            context.getCounter("LPA", "CHANGED").increment(1);
        }

        // Emit: nodeId \t newLabel \t neighbor1,neighbor2,...
        context.write(key, new Text(newLabel + "\t" + neighbors));
    }

    private int tieBreak(String a, String b) {
        // try numeric compare
        try {
            long ai = Long.parseLong(a);
            long bi = Long.parseLong(b);
            return Long.compare(ai, bi);
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
}

