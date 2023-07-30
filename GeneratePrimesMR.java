import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GeneratePrimesMR {

    // Mapper để tìm số nguyên tố trong mỗi khoảng [start, end]
    public static class PrimeMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable number = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Đọc dữ liệu đầu vào và chia thành hai phần: start và end
            String line = value.toString();
            String[] parts = line.split("\\s+");
            int start = Integer.parseInt(parts[0]);
            int end = Integer.parseInt(parts[1]);

            // Khởi tạo mảng boolean để lưu trữ trạng thái là số nguyên tố hay không
            boolean[] primes = new boolean[end - start + 1];
            for (int i = 0; i < primes.length; i++) {
                primes[i] = true;
            }

            // Xét trường hợp start = 1, ta không xét số 1 là số nguyên tố
            if (start == 1) {
                primes[0] = false;
            }

            // Sàng Eratosthenes để tìm số nguyên tố trong khoảng [start, end]
            for (int i = 2; i * i <= end; i++) {
                int j = (int) Math.max(i * i, Math.ceil(start / (double) i) * i);
                if (j < start) {
                    j += i;
                }
                while (j <= end) {
                    primes[j - start] = false;
                    j += i;
                }
            }

            // Ghi kết quả vào Context nếu là số nguyên tố
            for (int i = start; i <= end; i++) {
                if (primes[i - start]) {
                    outputKey.set("primes");
                    number.set(i);
                    context.write(outputKey, number);
                }
            }
        }
    }

    // Reducer để thu thập các số nguyên tố từ các Mapper và sắp xếp chúng
    public static class PrimeReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Integer> primes = new ArrayList<>();
            for (IntWritable value : values) {
                primes.add(value.get());
            }
            Collections.sort(primes);
            result.set(primes.toString());
            context.write(null, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // Cấu hình Hadoop
        Configuration conf = new Configuration();
        // Tạo một công việc (Job) có tên "GeneratePrimesMR"
        Job job = Job.getInstance(conf, "GeneratePrimesMR");
        // Thiết lập lớp chứa phương thức main là lớp chính của công việc
        job.setJarByClass(GeneratePrimesMR.class);
        // Thiết lập lớp Mapper sẽ được sử dụng
        job.setMapperClass(PrimeMapper.class);
        // Thiết lập lớp Reducer sẽ được sử dụng
        job.setReducerClass(PrimeReducer.class);
        // Thiết lập kiểu dữ liệu đầu ra của Mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Thiết lập đường dẫn thư mục đầu vào và thư mục đầu ra từ tham số dòng lệnh
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Chờ đến khi công việc hoàn thành và sau đó kết thúc chương trình
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
