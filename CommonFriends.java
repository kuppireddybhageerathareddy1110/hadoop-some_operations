import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriends {
    public static class Map
            extends Mapper<Object, Text, Text, Text> {
        private Text pair = new Text();
        private Text friends = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(":");
            if (parts.length == 2) {
                String user = parts[0];
                String[] friendList = parts[1].split(",");
                friends.set(parts[1]);
                // Emit for each pair of friends (but here, for edge pairs)
                for (String friend : friendList) {
                    String[] pairArray = {user, friend};
                    Arrays.sort(pairArray);
                    pair.set(pairArray[0] + "-" + pairArray[1]);
                    context.write(pair, friends);
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            Set<String> commonFriends = new HashSet<>();
            List<String> friendLists = new ArrayList<>();
            for (Text val : values) {
                friendLists.add(val.toString());
            }
            if (friendLists.size() == 2) {
                String[] friends1 = friendLists.get(0).split(",");
                String[] friends2 = friendLists.get(1).split(",");
                Set<String> set1 = new HashSet<>(Arrays.asList(friends1));
                Set<String> set2 = new HashSet<>(Arrays.asList(friends2));
                set1.retainAll(set2);
                // Remove the pair users themselves if present
                String[] pairUsers = key.toString().split("-");
                set1.remove(pairUsers[0]);
                set1.remove(pairUsers[1]);
                result.set(String.join(",", set1));
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "common friends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
