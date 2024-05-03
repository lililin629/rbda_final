import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TopDirectorsByRevenueMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Text director = new Text();
    private LongWritable revenue = new LongWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 3 && !parts[3].equals("Movie Revenue (M$)")) {
            try {
                if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("None")) {
                    long rev = Long.parseLong(parts[3]); // Revenue is assumed to be in the fourth column
                    String dir = parts[6].replaceAll("\\[|\\]|'", ""); // Clean director's name from brackets and quotes
                    director.set(dir);
                    revenue.set(rev);
                    context.write(director, revenue);
                }
            } catch (NumberFormatException e) {
                // Skip if revenue is not a number
            }
        }
    }
}
