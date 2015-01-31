import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QryopIlNear extends QryopIl {

	private int gap;

	public QryopIlNear(int gap, Qryop... qryops) {
		for (Qryop q : qryops) {
			this.args.add(q);
		}
		this.gap = gap;
	}

	@Override
	public void add(Qryop q) throws IOException {
		this.args.add(q);
	}

	@Override
	public QryResult evaluate(RetrievalModel r) throws IOException {
		this.allocArgPtrs(r);
		this.syntaxCheckArgResults(argPtrs);

		QryResult result = new QryResult();

		ArgPtr first = argPtrs.get(0);

		/*
		 * Find the same doc id as "#and"
		 */
		out: for (; first.nextDoc < first.invList.postings.size(); first.nextDoc++) {
			int docId = first.invList.getDocid(first.nextDoc);
			for (int j = 1; j < argPtrs.size(); j++) {
				ArgPtr ptrj = argPtrs.get(j);
				while (true) {
					// no more docs, no more match
					if (ptrj.nextDoc >= ptrj.invList.postings.size()) {
						break out;
					}
					// the first doc does not match
					else if (ptrj.invList.getDocid(ptrj.nextDoc) > docId) {
						continue out;
					}
					// does not match, look at next
					else if (ptrj.invList.getDocid(ptrj.nextDoc) < docId)
						ptrj.nextDoc++;
					else {
						break;
					}
				}
			}
			List<Integer> locations = new ArrayList<Integer>();
			System.out.println("near matched" + first.invList.postings.get(first.nextDoc).docid);
			// all doc ids matched, look for #NEAR match
			nearEvaluate: 
			for (int firstPosition : first.invList.postings
					.get(first.nextDoc).positions) {
				int last = firstPosition;
				for (int j = 1; j < argPtrs.size(); j++) {
					InvList.DocPosting posting = argPtrs.get(j).invList.postings
							.get(argPtrs.get(j).nextDoc);
					while (true) {
						if (posting.position >= posting.positions.size()) {
							break nearEvaluate;
						}
						if (posting.positions.get(posting.position) < last) {
							posting.position++;
						} else if (posting.positions.get(posting.position)
								- last > gap) {
							continue nearEvaluate;
						} else {
							// a match between two adjacent words
							last = posting.positions.get(posting.position);
							break;
						}
					}
				}
				/*
				 * A near is successfully matched
				 */
				System.out.println("found");
				locations.add(last);
			}
			if (locations.size() > 0) {
				result.invertedList.appendPosting(docId, locations);
			}
		}
		freeArgPtrs();
		return result;

	}

	@Override
	public String toString() {
		String result = new String();

		for (Iterator<Qryop> i = this.args.iterator(); i.hasNext();)
			result += (i.next().toString() + " ");

		return ("#NEAR( " + result + ")");
	}

	/**
	 * syntaxCheckArgResults does syntax checking that can only be done after
	 * query arguments are evaluated.
	 * 
	 * @param ptrs
	 *            A list of ArgPtrs for this query operator.
	 * @return True if the syntax is valid, false otherwise.
	 */
	public Boolean syntaxCheckArgResults(List<ArgPtr> ptrs) {

		for (int i = 0; i < this.args.size(); i++) {

			if (!(this.args.get(i) instanceof QryopIl))
				QryEval.fatalError("Error:  Invalid argument in "
						+ this.toString());
			else if ((i > 0)
					&& (!ptrs.get(i).invList.field
							.equals(ptrs.get(0).invList.field)))
				QryEval.fatalError("Error:  Arguments must be in the same field:  "
						+ this.toString());
		}

		return true;
	}

}
