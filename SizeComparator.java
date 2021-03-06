import java.util.Comparator;

/**
 * A utility class for sorting the ArgPtr by the size of the inverted or score
 * lists.
 * 
 * @author siyuwei
 *
 */
public class SizeComparator implements Comparator<Qryop.ArgPtr> {

	@Override
	public int compare(Qryop.ArgPtr arg0, Qryop.ArgPtr arg1) {
		if (arg0.scoreList != null) {
			return arg0.scoreList.scores.size() - arg1.scoreList.scores.size();
		} else {
			return arg0.invList.postings.size() - arg1.invList.postings.size();
		}
	}

}
