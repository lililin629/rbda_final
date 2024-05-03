import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TopYearsByRevenueMapper extends Mapper<Object, Text, IntWritable, LongWritable> {
    private IntWritable year = new IntWritable();
    private LongWritable revenue = new LongWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 3 && !parts[1].equals("Unknown")) { // Avoid processing header
            try {
                int yr = Integer.parseInt(parts[1]); // Year is in the second column
                if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("None")) {
                    long rev = Long.parseLong(parts[3]); // Revenue is in the fourth column
                    year.set(yr);
                    revenue.set(rev);
                    context.write(year, revenue);
                }
            } catch (NumberFormatException e) {
                // Skip if year or revenue is not a number
            }
        }
    }
}
