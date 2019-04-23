import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Split_all {
	static HashSet<String> hashSet;
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		public void setup(Context context) {
			try {
				hashSet = Load_dir.load();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable ikey, Text ivalue, Context context)
				throws IOException, InterruptedException {
			Result result = NlpAnalysis.parse(ivalue.toString());
			List<Term> term = result.getTerms();
			Text key = new Text();
			HashSet<String> set=new HashSet();
			StringBuilder line=new StringBuilder();
			for (Term t : term) {
				if(hashSet.contains(t.getName())&&!set.contains(t.getName())) {
					if(t.getName()!=null&&t.getName().length()>0) {
						line.append(t.getName() + " ");
						set.add(t.getName());
					}
				}
			}
			key.set(line.toString());
			context.write(key, NullWritable.get());
		}
	}

	public static class Reduce extends Reducer<Text,NullWritable,Text,NullWritable>{
		public void reduce(Text ikey,Iterable<NullWritable> ivalues,Context context)
				throws IOException, InterruptedException {
			context.write(ikey,NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir","F:\\hadoop-2.7.6");
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Split_all.class);
		job.setJobName("split_all");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path path=new Path("hdfs://192.168.88.128:9000/output");
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.88.128:9000/input/jinyong"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.88.128:9000/output"));
		job.waitForCompletion(true);
	}

}

