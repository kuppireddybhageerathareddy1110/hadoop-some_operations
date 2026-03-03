import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommunityDetectionDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CommunityDetectionDriver <input path> <output path> <max iterations>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int maxIterations = Integer.parseInt(args[2]);

        String iterInput = inputPath;

        for (int iter = 1; iter <= maxIterations; iter++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Community Detection Iteration " + iter);

            job.setJarByClass(CommunityDetectionDriver.class);
            job.setMapperClass(CommunityMapper.class);
            job.setReducerClass(CommunityReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(iterInput));
            String iterOutput = outputPath + "-iter" + iter;
            FileOutputFormat.setOutputPath(job, new Path(iterOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Iteration " + iter + " failed");
                System.exit(1);
            }

            long changed = job.getCounters().findCounter("LPA", "CHANGED").getValue();
            System.out.println("Iteration " + iter + " -> changed nodes = " + changed);

            if (changed == 0) {
                System.out.println("Convergence reached after " + iter + " iterations.");
                break;
            }

            // Next iteration reads the last iteration's output directory
            iterInput = iterOutput;
        }
    }
}
