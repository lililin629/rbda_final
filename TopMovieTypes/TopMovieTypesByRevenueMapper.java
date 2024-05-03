import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TopMovieTypesByRevenueMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Text movieType = new Text();
    private LongWritable revenue = new LongWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 3 && !parts[2].equals("Movie Type")) { // Avoid processing header
            try {
                if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("None")) {
                    long rev = Long.parseLong(parts[3]); // Revenue is in the fourth column
                    movieType.set(parts[2].replaceAll("\"", "")); // Clean movie type from extra quotes
                    revenue.set(rev);
                    context.write(movieType, revenue);
                }
            } catch (NumberFormatException e) {
                // Skip if revenue is not a number
            }
        }
    }
}
