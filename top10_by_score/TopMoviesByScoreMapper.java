import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TopMoviesByScoreMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable score = new DoubleWritable();
    private Text movie = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 4) {
            try {
                double sc = Double.parseDouble(parts[4]); // Assuming score is in the 5th column
                movie.set(parts[0]); // Assuming movie name is in the first column
                score.set(sc);
                context.write(new DoubleWritable(-sc), movie); // Negate to sort descending
            } catch (NumberFormatException e) {
                // handle the case where score is not a double
            }
        }
    }
}
