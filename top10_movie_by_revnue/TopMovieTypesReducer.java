import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;

public class TopMovieTypesReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Long, Text> top10 = new TreeMap<Long, Text>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text val : values) {
            // parse the input data to get moveiNmae and revenue
            String[] parts = val.toString().split(",");
            String movieName = parts[0];
            long revenue = Long.parseLong(parts[1]);
            // Add movie name and revenue to our map with the revenue as the key
            top10.put(revenue, new Text(movieName + "," + String.valueOf(revenue)));
            if (top10.size() > 10) {
                top10.remove(top10.firstKey());
            }
        }
        for (Text t : top10.descendingMap().values()) {
            // Output our ten records to the file system
            // with a null key
            context.write(NullWritable.get(), t);
        }

    }

}
