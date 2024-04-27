import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;

public class TopMovieTypesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, String> topMovieTypes = new TreeMap<>();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        topMovieTypes.put(sum, key.toString());

        // Only keep top 10
        if (topMovieTypes.size() > 10) {
            topMovieTypes.remove(topMovieTypes.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Long key : topMovieTypes.descendingKeySet()) {
            context.write(new Text(topMovieTypes.get(key)), new LongWritable(key));
        }
    }
}
