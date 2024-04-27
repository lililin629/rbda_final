import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

public class MovieRevenueMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header
        if (key.get() == 0 && value.toString().contains("Movie Name"))
            return;

        // Improved parsing to handle commas within quotes
        String line = value.toString();
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        try {
            String movieType = parts[4].replaceAll("[\\[\\]' ]", "").trim(); // Clean up the movie type field
            long revenue = (long) (Double.parseDouble(parts[6].trim()) * 1000000); // Convert revenue to long for
                                                                                   // stability in large numbers
            String actorsLine = parts[11]; // Extracting the actors field

            // Parse actors removing the enclosing brackets and quotes
            String[] actors = actorsLine.replaceAll("[\\[\\]']", "").split(",\\s*");

            context.write(new Text(Arrays.toString(actors)), new LongWritable(revenue)); // Example usage
        } catch (NumberFormatException e) {
            // Handle parse error or ignore the line
        }
    }
}
