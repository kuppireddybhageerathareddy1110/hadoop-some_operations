import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ModularityUtils {

    public static double readModularityFromHdfs(String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(path + "/part-r-00000");

        if (!fs.exists(p)) {
            System.err.println("Modularity file not found: " + p.toString());
            return 0.0;
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        double modularity = 0.0;

        while ((line = br.readLine()) != null) {
            // Expect format: communityId \t modularityValue
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                modularity += Double.parseDouble(parts[1]);
            }
        }
        br.close();

        return modularity;
    }
}
