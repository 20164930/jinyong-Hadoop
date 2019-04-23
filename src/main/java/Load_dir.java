
import org.ansj.library.DicLibrary;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;

public class Load_dir {
	public static HashSet<String> load() throws Exception{
		HashSet<String> list=new HashSet<>();
		File file=new File("jinyong_all_person.txt");
		FileReader fr=new FileReader(file);
		BufferedReader br=new BufferedReader(fr);
		String s;
		while((s=br.readLine())!=null){
			list.add(s);
			DicLibrary.insert(DicLibrary.DEFAULT, s, "n", 1000);//设置自定义分词  n代表名词 1000代表默认出现的频率
		}
		br.close();
		fr.close();
		return list;
	}
}
