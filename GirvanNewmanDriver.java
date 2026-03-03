import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GirvanNewmanDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: GirvanNewmanDriver <inputEdges> <outputDir> <maxIter> <removeTopPercent>");
            System.exit(1);
        }

        String inputEdges = args[0];           // initial edges file (nodeA \t nodeB)
        String outputDir = args[1];
        int maxIter = Integer.parseInt(args[2]);
        double removeTopPercent = Double.parseDouble(args[3]); // e.g., 10.0 for top 10%

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String currentGraph = inputEdges;
        double bestModularity = Double.NEGATIVE_INFINITY;
        String bestCommunities = null;

        for (int iter = 0; iter < maxIter; iter++) {
            System.out.println("=== Iteration " + iter + " ===");

            String betPath = outputDir + "/betweenness_iter_" + iter;
            // 1) compute edge betweenness
            {
                Job job = Job.getInstance(conf, "BetweennessIter" + iter);
                job.setJarByClass(GirvanNewmanDriver.class);
                job.setMapperClass(BetweennessMapper.class);
                job.setReducerClass(BetweennessReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(currentGraph));
                FileOutputFormat.setOutputPath(job, new Path(betPath));

                if (!job.waitForCompletion(true)) {
                    System.err.println("Betweenness job failed");
                    System.exit(1);
                }
            }

            // 2) remove top betweenness edges
            String updatedGraph = outputDir + "/graph_iter_" + iter;
            {
                Job job = Job.getInstance(conf, "EdgeRemovalIter" + iter);
                job.setJarByClass(GirvanNewmanDriver.class);
                job.setMapperClass(EdgeRemovalMapper.class);
                job.setReducerClass(EdgeRemovalReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // feed both graph and betweenness as inputs
                FileInputFormat.addInputPath(job, new Path(currentGraph));
                FileInputFormat.addInputPath(job, new Path(betPath));
                FileOutputFormat.setOutputPath(job, new Path(updatedGraph));

                job.getConfiguration().setDouble("girvan.removeTopPercent", removeTopPercent);

                if (!job.waitForCompletion(true)) {
                    System.err.println("Edge removal job failed");
                    System.exit(1);
                }
            }

            // 3) detect communities (connected components) — iterative CC that finishes quickly for small graphs
            String communitiesPath = outputDir + "/communities_iter_" + iter;
            {
                Job job = Job.getInstance(conf, "CCIter" + iter);
                job.setJarByClass(GirvanNewmanDriver.class);
                job.setMapperClass(ConnectedComponentsMapper.class);
                job.setReducerClass(ConnectedComponentsReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(updatedGraph));
                FileOutputFormat.setOutputPath(job, new Path(communitiesPath));

                if (!job.waitForCompletion(true)) {
                    System.err.println("Community detection job failed");
                    System.exit(1);
                }
            }

            // 4) modularity calculation
            String modularityOut = outputDir + "/modularity_iter_" + iter;
            double modularity = Double.NEGATIVE_INFINITY;
            {
                Job job = Job.getInstance(conf, "ModularityIter" + iter);
                job.setJarByClass(GirvanNewmanDriver.class);
                job.setMapperClass(ModularityMapper.class);
                job.setReducerClass(ModularityReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);

                // input: graph (updatedGraph) and communities (communitiesPath)
                FileInputFormat.addInputPath(job, new Path(updatedGraph));
                FileInputFormat.addInputPath(job, new Path(communitiesPath));
                FileOutputFormat.setOutputPath(job, new Path(modularityOut));

                if (!job.waitForCompletion(true)) {
                    System.err.println("Modularity job failed");
                    System.exit(1);
                }

                // For simplicity we expect the Reducer to write a single line with total modularity to part-r-00000
                // Read it from HDFS (or parse job output). Here we do a helper function to read that file.
                modularity = ModularityUtils.readModularityFromHdfs(modularityOut, conf);
            }

            System.out.println("Modularity at iter " + iter + " = " + modularity);
            if (modularity > bestModularity) {
                bestModularity = modularity;
                bestCommunities = communitiesPath;
            }

            // stop if graph becomes disconnected to singletons or no edges removed
            if (fs.exists(new Path(updatedGraph + "/_SUCCESS"))) {
                // prepare next
                currentGraph = updatedGraph;
            } else {
                System.out.println("Updated graph not present, stopping");
                break;
            }
        }

        System.out.println("Best modularity: " + bestModularity);
        System.out.println("Best communities path: " + bestCommunities);
    }
}
