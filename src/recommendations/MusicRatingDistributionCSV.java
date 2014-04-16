package recommendations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/*
 * hadoop jar /home/jeet/Documents/realtime/MusicRecommender/target/MusicRecommender-0.0.1-SNAPSHOT.jar recommendations.job.hbase.RecommendationInsertJob /user/hduser/reco/output_sample
 */
public class MusicRatingDistributionCSV {
	public static void main(String args[]) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("data/rating_dis"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("data/rating_dis.csv"));

		String line;
		while((line = br.readLine()) != null) {
		String[] values = line.split("\\t", -1);
		bw.write(values[0] + "," + values[1] + "\n");
		}

		br.close();
		bw.close();
	}
}
