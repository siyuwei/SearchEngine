import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
		return evaluateLinear(r);
	}

	/**
	 * Evaluate the or operator by finding the smallest current doc id, same as
	 * the approach for "#Syn".
	 * 
	 * @param r
	 *            retrieval model
	 * @return
	 * @throws IOException
	 */
	public QryResult evaluateLinear(RetrievalModel r) throws IOException {
		super.allocArgPtrs(r);
		QryResult result = new QryResult();

		while (this.argPtrs.size() > 0) {

			int nextDocid = getSmallestCurrentDocid();

			// Create a new posting that is the union of the posting lists
			// that match the nextDocid.

			double score = 0;
			for (int i = 0; i < this.argPtrs.size(); i++) {
				ArgPtr ptri = this.argPtrs.get(i);

				/*
				 * There might be inverted list where there is no match at all
				 */
				if (ptri.nextDoc < ptri.scoreList.scores.size()
						&& ptri.scoreList.getDocid(ptri.nextDoc) == nextDocid) {
					score = Math.max(score,
							ptri.scoreList.getDocidScore(ptri.nextDoc));
					ptri.nextDoc++;
				}
			}

			result.docScores.add(nextDocid, score);

			// If an ArgPtr has reached the end of its list, remove it.
			// The loop is backwards so that removing an arg does not
			// interfere with iteration.

			for (int i = this.argPtrs.size() - 1; i >= 0; i--) {
				ArgPtr ptri = this.argPtrs.get(i);

				if (ptri.nextDoc >= ptri.scoreList.scores.size()) {
					this.argPtrs.remove(i);
				}
			}
		}

		// free arg ptrs
		freeArgPtrs();

		return result;
	}

	/**
	 * Evaluate the or operator by using hash map and then sort the output
	 * 
	 * @param r
	 *            retrieval model
	 * @return the result
	 * @throws IOException
	 */
	public QryResult evaluateHashMap(RetrievalModel r) throws IOException {

		super.allocArgPtrs(r);
		QryResult result = new QryResult();

		/*
		 * Build the map, every entry maps from a doc id to its score
		 */
		Map<Integer, Integer> idToScore = new HashMap<Integer, Integer>();
		for (ArgPtr ptr : this.argPtrs) {
			for (ScoreList.ScoreListEntry entry : ptr.scoreList.scores) {
				Integer score = idToScore.get(entry.docid);
				if (score != null) {
					idToScore.put(entry.docid,
							Math.max(score, (int) entry.score));
				} else {
					idToScore.put(entry.docid, (int) entry.score);
				}
			}
		}

		/*
		 * Get
		 */
		List<Entry> list = new ArrayList<Entry>();
		for (int i : idToScore.keySet()) {
			list.add(new Entry(i, idToScore.get(i)));
		}

		Collections.sort(list);

		for (Entry e : list) {
			result.docScores.add(e.key, e.value);
		}

		// free arg ptrs
		freeArgPtrs();

		return result;
	}

	/**
	 * Evaluate the or operator with a tree map approach
	 * 
	 * @param r
	 *            the retrieval model
	 * @return the result
	 * @throws IOException
	 */
	public QryResult evaluateTreeMap(RetrievalModel r) throws IOException {

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

		/*
		 * Adding all the entries to the result according to the order of doc id
		 */
		for (Map.Entry<Integer, Integer> e : idToScore.entrySet()) {
			result.docScores.add(e.getKey(), e.getValue());
		}
		// free arg ptrs
		freeArgPtrs();

		return result;
	}

	/**
	 * A helper class used for sorting in the hash map approach
	 * 
	 * @author siyuwei
	 *
	 */
	private static class Entry implements Comparable<Entry> {

		public Entry(int key, int value) {
			this.key = key;
			this.value = value;
		}

		private int key;
		private int value;

		@Override
		public int compareTo(Entry arg0) {
			return this.key - arg0.key;
		}

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

	/**
	 * Return the smallest unexamined docid from the ArgPtrs.
	 * 
	 * @return The smallest internal document id.
	 */
	public int getSmallestCurrentDocid() {

		int nextDocid = Integer.MAX_VALUE;

		for (int i = 0; i < this.argPtrs.size(); i++) {
			ArgPtr ptri = this.argPtrs.get(i);
			if (ptri.nextDoc < ptri.scoreList.scores.size()
					&& nextDocid > ptri.scoreList.getDocid(ptri.nextDoc))
				nextDocid = ptri.scoreList.getDocid(ptri.nextDoc);
		}

		return (nextDocid);
	}

}
