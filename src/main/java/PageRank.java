import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PageRank {
    public static class RankMap extends Mapper<LongWritable, Text,Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int index_dollar = line.indexOf("$");
            if (index_dollar != -1){
                line = line.substring(index_dollar+1);
            }
            int index_t = line.indexOf("\t");
            int index_j = line.indexOf("#");
            double PR = Double.parseDouble(line.substring(index_t+1,index_j));
            String name = line.substring(0,index_t);
            String names = line.substring(index_j+1);
            for (String name_value:names.split(";")){
                String[] info = name_value.split(":");
                double relation = Double.parseDouble(info[1]);
                double cal = PR * relation;
                context.write(new Text(info[0]),new Text(String.valueOf(cal)));
            }
            context.write(new Text(name),new Text("#"+line.substring(index_j+1)));
        }
    }

    public static class RankReduce extends Reducer<Text, Text,Text, Text> {
        int index=0;
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nameList = "";
            double count = 0;
            for (Text text : values){
                String t = text.toString();
                if (t.charAt(0) == '#'){
                    nameList = t;
                }else{
                    count += Double.parseDouble(t);
                }
            }
            index++;
            context.write(new Text(index+"$"+key.toString()),new Text(String.valueOf(count) + nameList));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","F:\\hadoop-2.7.6");
        for(int i=0;i<10;i++){
            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1);
            job1.setJarByClass(PageRank.class);
            job1.setJobName("Page_Rank1");
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setMapperClass(RankMap.class);
            job1.setReducerClass(RankReduce.class);
            if(i==0) {
                Path path1 = new Path("hdfs://192.168.88.128:9000/pageRank");
                FileSystem fileSystem1 = path1.getFileSystem(conf1);
                if (fileSystem1.exists(path1)) {
                    fileSystem1.delete(path1, true);
                }
                FileInputFormat.addInputPath(job1, new Path("hdfs://192.168.88.128:9000/power/part-r-00000"));
                FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.88.128:9000/pageRank"));
            }else{
                String from,to;
                if(i%2==0){
                    from="pageRank0";
                    to="pageRank";
                }else{
                    from="pageRank";
                    to="pageRank0";
                }
                Path path1 = new Path("hdfs://192.168.88.128:9000/"+to);
                FileSystem fileSystem1 = path1.getFileSystem(conf1);
                if (fileSystem1.exists(path1)) {
                    fileSystem1.delete(path1, true);
                }
                FileInputFormat.addInputPath(job1, new Path("hdfs://192.168.88.128:9000/"+from+"/part-r-00000"));
                FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.88.128:9000/"+to));
            }
            job1.waitForCompletion(true);
        }
    }
}
