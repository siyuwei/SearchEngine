import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class implements the "OR" operator
 * 
 * @author siyuwei
 *
 */

public class QryopSlOr extends QryopSl {

	public QryopSlOr(Qryop... q) {
		for (Qryop o : q) {
			this.args.add(o);
		}
	}

	@Override
	public void add(Qryop q) throws IOException {
		this.args.add(q);

	}

	@Override
	public QryResult evaluate(RetrievalModel r) throws IOException {

		super.allocArgPtrs(r);
		QryResult result = new QryResult();

		/*
		 * For each doc id, choose the maximum score as its score. As the output
		 * needs to be sorted by doc id, tree map is used to keep the order
		 * sorted
		 */
		Map<Integer, Integer> idToScore = new TreeMap<Integer, Integer>();
		for (ArgPtr ptr : this.argPtrs) {
			for (int i = 0; i < ptr.scoreList.scores.size(); i++) {
				int id = ptr.scoreList.getDocid(i);
				int score = (int) ptr.scoreList.getDocidScore(i);
				Integer currScore = idToScore.get(id);
				if (currScore == null) {
					idToScore.put(id, score);
				} else {
					if (r instanceof RetrievalModelRankedBoolean)
						idToScore.put(id, Math.max(score, currScore));
					else
						idToScore.put(id, 1);
				}
			}
		}

		for (Map.Entry<Integer, Integer> e : idToScore.entrySet()) {
			result.docScores.add(e.getKey(), e.getValue());
		}
		// free arg ptrs
		freeArgPtrs();

		return result;
	}

	@Override
	public String toString() {
		String result = new String();

		for (int i = 0; i < this.args.size(); i++)
			result += this.args.get(i).toString() + " ";

		return ("#OR( " + result + ")");
	}

	@Override
	public double getDefaultScore(RetrievalModel r, long docid)
			throws IOException {

		return 0.0;
	}

}
