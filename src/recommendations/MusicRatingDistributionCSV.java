package recommendations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

/*
 * hadoop jar /home/jeet/Documents/realtime/MusicRecommender/target/MusicRecommender-0.0.1-SNAPSHOT.jar recommendations.job.hbase.RecommendationInsertJob /user/hduser/reco/output_sample
 */
public class MusicRatingDistributionCSV {
	public static void main1(String args[]) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("data/kaggle_visible_evaluation_triplets.txt"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("data/triplets.csv"));

		String line;
		while((line = br.readLine()) != null) {
			String[] values = line.split("\\t", -1);
			bw.write(values[0] + "," + values[1] + "," + values[2] + "\n");
		}
		br.close();
		bw.close();
	}
	public static void main(String args[]) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("data/kaggle_songs.txt"));
		Map<String, String> usersList = new HashMap<String, String>();
		String line;
		while((line = br.readLine()) != null) {
			String[] values = line.split(" ", -1);
			usersList.put(values[0], values[1]);
		}
		br.close();
		String key = usersList.entrySet().iterator().next().getKey();
		System.out.println(usersList.size());
		System.out.println(key);
		System.out.println(usersList.get(key));
	}
}
