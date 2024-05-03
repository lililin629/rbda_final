import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgRevenuePerMovieMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text movieName = new Text();
    private FloatWritable revenue = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 3 && !parts[0].equals("Movie Name")) { // Avoid processing header
            try {
                movieName.set(parts[0]);
                if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("None")) {
                    revenue.set(Float.parseFloat(parts[3]));
                    context.write(movieName, revenue);
                }
            } catch (NumberFormatException e) {
                // Skip if revenue is not a number
            }
        }
    }
}
