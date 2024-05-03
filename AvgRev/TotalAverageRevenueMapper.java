import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TotalAverageRevenueMapper extends Mapper<Object, Text, Text, LongWritable> {
    private static final Text CONSTANT_KEY = new Text("AverageRevenue");
    private LongWritable revenue = new LongWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 3 && !parts[0].equals("Movie Name")) { // Avoid processing header
            try {
                if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("None")) {
                    revenue.set(Long.parseLong(parts[3]));
                    context.write(CONSTANT_KEY, revenue);
                }
            } catch (NumberFormatException e) {
                // Skip if revenue is not a number
            }
        }
    }
}
