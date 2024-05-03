import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TopMoviesByScoreReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    private int count = 0;

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            if (count < 10) {
                context.write(value, new DoubleWritable(-key.get())); // Revert the score to positive
                count++;
            } else {
                break;
            }
        }
    }
}
