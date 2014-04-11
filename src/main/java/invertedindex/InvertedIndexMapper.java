package invertedindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private List<Text> stopWords;
	private final Pattern onlyAlphaNumericPattern = Pattern
			.compile("[^A-Za-z0-9 ]");
	private final Pattern pageIdPattern = Pattern.compile("<id>(.+?)</id>");

	public InvertedIndexMapper() throws IOException {
		// stopwords 리스트 생성
		stopWords = new ArrayList<Text>();
		BufferedReader br = new BufferedReader(new FileReader(new File(
				"stopwords_v3.txt")));
		String line;
		while ((line = br.readLine()) != null) {
			stopWords.add(new Text(line));
		}
		br.close();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String page = value.toString();
		String sanitizedPage = getSanitizedPage(page);
		IntWritable pageId = new IntWritable(getPageID(page));

		StringTokenizer iter = new StringTokenizer(sanitizedPage);
		while (iter.hasMoreTokens()) {
			Text word = new Text();
			word.set(iter.nextToken());

			if (!stopWords.contains(word)) {
				context.write(word, pageId);
			}
		}
	}

	// ** Jsoup 사용하여 HTML 태그를 사전에 제거
	public String getSanitizedPage(String page) {
		String rawText = Jsoup.parse(page).text();
		return onlyAlphaNumericPattern.matcher(rawText).replaceAll(" ")
				.toLowerCase();
	}

	public Integer getPageID(String page) {
		Matcher matcher = pageIdPattern.matcher(page);
		matcher.find();
		return new Integer(matcher.group(1));
	}
}
