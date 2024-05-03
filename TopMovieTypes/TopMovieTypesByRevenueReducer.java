import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

public class TopMovieTypesByRevenueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, String> topMovieTypes = new TreeMap<>();

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        topMovieTypes.put(sum, key.toString());
        if (topMovieTypes.size() > 10) {
            topMovieTypes.remove(topMovieTypes.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Entry<Long, String> entry : topMovieTypes.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }
}
