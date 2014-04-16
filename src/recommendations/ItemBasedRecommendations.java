package recommendations;

import java.util.List;

import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity;

import com.google.common.primitives.Longs;

public class ItemBasedRecommendations {
	
	public static final String NUM_RECOMMENDATIONS = "10";
	public static final String SIMILARITY_CLASS_NAME = CosineSimilarity.class.getName();
	
	/*
	 * hadoop jar /home/jeet/Downloads/Mahout/mahout-distribution-0.8/core/target/mahout-core-0.8-job.jar org.apache.mahout.cf.taste.hadoop.item.RecommenderJob -Dmapred.input.dir=/user/hduser/reco/movies/input -Dmapred.output.dir=/user/hduser/reco/movies/output --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity --numRecommendations 10
	 */
	/*
	 * hadoop jar /home/jeet/Documents/realtime/MusicRecommender/target/MusicRecommender-0.0.1-SNAPSHOT.jar recommendations.ItemBasedRecommendations /user/hduser/reco/movies/input /user/hduser/reco/movies/output-by-jar
	 */
	public static void main(String args[]) throws Exception {
		String newArgs [] = new String[6];
		newArgs[0] = "-Dmapred.input.dir="+args[0];
		newArgs[1] = "-Dmapred.output.dir=" + args[1];
		newArgs[2] = "--similarityClassname";
		newArgs[3] = SIMILARITY_CLASS_NAME;
		newArgs[4] = "--numRecommendations";
		newArgs[5] = NUM_RECOMMENDATIONS;
		long [] bw = new long[2];
		bw[0] = 100l;
		bw[1] = 101l;
		List<Long> longs = Longs.asList(bw[0], bw[1]);
		System.out.println(longs.get(0));
		System.out.println(longs.get(1));
		RecommenderJob.main(newArgs);
	}

}
