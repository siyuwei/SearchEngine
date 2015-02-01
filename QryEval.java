/**
 *  QryEval illustrates the architecture for the portion of a search
 *  engine that evaluates queries.  It is a template for class
 *  homework assignments, so it emphasizes simplicity over efficiency.
 *  It implements an unranked Boolean retrieval model, however it is
 *  easily extended to other retrieval models.  For more information,
 *  see the ReadMe.txt file.
 *
 *  Copyright (c) 2015, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class QryEval {

	static String usage = "Usage:  java "
			+ System.getProperty("sun.java.command") + " paramFile\n\n";

	// The index file reader is accessible via a global variable. This
	// isn't great programming style, but the alternative is for every
	// query operator to store or pass this value, which creates its
	// own headaches.

	public static IndexReader READER;

	// Create and configure an English analyzer that will be used for
	// query parsing.

	public static EnglishAnalyzerConfigurable analyzer = new EnglishAnalyzerConfigurable(
			Version.LUCENE_43);
	static {
		analyzer.setLowercase(true);
		analyzer.setStopwordRemoval(true);
		analyzer.setStemmer(EnglishAnalyzerConfigurable.StemmerType.KSTEM);
	}

	/**
	 * @param args
	 *            The only argument is the path to the parameter file.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();

		// must supply parameter file
		if (args.length < 1) {
			System.err.println(usage);
			System.exit(1);
		}

		// read in the parameter file; one parameter per line in format of
		// key=value
		Map<String, String> params = new HashMap<String, String>();
		Scanner scan = new Scanner(new File(args[0]));
		String line = null;
		do {
			line = scan.nextLine();
			String[] pair = line.split("=");
			params.put(pair[0].trim(), pair[1].trim());
		} while (scan.hasNext());
		scan.close();

		// parameters required for this example to run
		if (!params.containsKey("indexPath")) {
			System.err.println("Error: Parameters were missing.");
			System.exit(1);
		}

		// open the index
		READER = DirectoryReader.open(FSDirectory.open(new File(params
				.get("indexPath"))));

		if (READER == null) {
			System.err.println(usage);
			System.exit(1);
		}

		/**
		 * The index is open. Start evaluating queries. The examples below show
		 * query trees for two simple queries. These are meant to illustrate how
		 * query nodes are created and connected. However your software will not
		 * create queries like this. Your software will use a query parser. See
		 * parseQuery.
		 *
		 * The general pattern is to tokenize the query term (so that it gets
		 * converted to lowercase, stopped, stemmed, etc), create a Term node to
		 * fetch the inverted list, create a Score node to convert an inverted
		 * list to a score list, evaluate the query, and print results.
		 * 
		 * Modify the software so that you read a query from a file, parse it,
		 * and form the query tree automatically.
		 */

		/*
		 * Open query file
		 */
		if (!params.containsKey("queryFilePath")) {
			System.err.println("Error: Parameters were missing.");
			System.exit(1);
		}

		List<String> queries = new ArrayList<String>();
		try {
			BufferedReader queryReader = new BufferedReader(new FileReader(
					params.get("queryFilePath")));
			while (true) {
				String temp = queryReader.readLine();
				if (temp == null) {
					break;
				}
				queries.add(temp);
			}
			queryReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(usage);
			System.exit(1);
		}

		RetrievalModel r = null;
		String retrievalModel = params.get("retrievalAlgorithm");
		try {
			Class<?> c = Class.forName("RetrievalModel" + retrievalModel);
			r = (RetrievalModel) c.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(usage);
			System.exit(1);
		}

		String outputFile = params.get("trecEvalOutputPath");
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(outputFile));
			for (String query : queries) {
				for (String entry : outputEntry(query, r)) {
					writer.println(entry);
				}
			}
			writer.close();
		} catch (IOException e) {
			System.err.println(usage);
			System.exit(1);
		}

		/*
		 * Create the trec_eval output. Your code should write to the file
		 * specified in the parameter file, and it should write the results that
		 * you retrieved above. This code just allows the testing infrastructure
		 * to work on QryEval.
		 */

		// Later HW assignments will use more RAM, so you want to be aware
		// of how much memory your program uses.

		printMemoryUsage(false);

		System.out.println("time: " + (System.currentTimeMillis() - start)
				/ 1000);

	}

	/**
	 * Write an error message and exit. This can be done in other ways, but I
	 * wanted something that takes just one statement so that it is easy to
	 * insert checks without cluttering the code.
	 * 
	 * @param message
	 *            The error message to write before exiting.
	 * @return void
	 */
	static void fatalError(String message) {
		System.err.println(message);
		System.exit(1);
	}

	/**
	 * Get the external document id for a document specified by an internal
	 * document id. If the internal id doesn't exists, returns null.
	 * 
	 * @param iid
	 *            The internal document id of the document.
	 * @throws IOException
	 */
	static String getExternalDocid(int iid) throws IOException {
		Document d = QryEval.READER.document(iid);
		String eid = d.get("externalId");
		return eid;
	}

	/**
	 * Finds the internal document id for a document specified by its external
	 * id, e.g. clueweb09-enwp00-88-09710. If no such document exists, it throws
	 * an exception.
	 * 
	 * @param externalId
	 *            The external document id of a document.s
	 * @return An internal doc id suitable for finding document vectors etc.
	 * @throws Exception
	 */
	static int getInternalDocid(String externalId) throws Exception {
		Query q = new TermQuery(new Term("externalId", externalId));

		IndexSearcher searcher = new IndexSearcher(QryEval.READER);
		TopScoreDocCollector collector = TopScoreDocCollector.create(1, false);
		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		if (hits.length < 1) {
			throw new Exception("External id not found.");
		} else {
			return hits[0].doc;
		}
	}

	/**
	 * parseQuery converts a query string into a query tree.
	 * 
	 * @param qString
	 *            A string containing a query.
	 * @param qTree
	 *            A query tree
	 * @throws IOException
	 */
	static Qryop parseQuery(String qString) throws IOException {

		Qryop currentOp = null;
		Stack<Qryop> stack = new Stack<Qryop>();

		// Add a default query operator to an unstructured query. This
		// is a tiny bit easier if unnecessary whitespace is removed.

		qString = qString.trim();

		// if (qString.charAt(0) != '#') {
		qString = "#or(" + qString + ")";
		// }

		// Tokenize the query.

		StringTokenizer tokens = new StringTokenizer(qString, "\t\n\r ,()",
				true);
		String token = null;

		// Each pass of the loop processes one token. To improve
		// efficiency and clarity, the query operator on the top of the
		// stack is also stored in currentOp.

		while (tokens.hasMoreTokens()) {

			token = tokens.nextToken();
			if (token.matches("[ ,(\t\n\r]")) {
				// Ignore most delimiters.
			} else if (token.equalsIgnoreCase("#and")) {
				currentOp = new QryopSlAnd();
				stack.push(currentOp);
			} else if (token.equalsIgnoreCase("#syn")) {
				currentOp = new QryopIlSyn();
				stack.push(currentOp);
			} else if (token.equalsIgnoreCase("#or")) {
				currentOp = new QryopSlOr();
				stack.push(currentOp);
			} else if (token.startsWith("#near") || token.startsWith("#NEAR")) {
				String[] temp = token.split("/");
				if (temp.length < 2) {
					System.err.println("Wrong operator usage");
					return null;
				}
				currentOp = new QryopIlNear(Integer.parseInt(temp[1]));
				stack.push(currentOp);
			} else if (token.startsWith(")")) { // Finish current query

				// operator.
				// If the current query operator is not an argument to
				// another query operator (i.e., the stack is empty when it
				// is removed), we're done (assuming correct syntax - see
				// below). Otherwise, add the current operator as an
				// argument to the higher-level operator, and shift
				// processing back to the higher-level operator.

				stack.pop();

				if (stack.empty())
					break;

				Qryop arg = currentOp;
				currentOp = stack.peek();
				currentOp.add(arg);
			} else {

				// NOTE: You should do lexical processing of the token before
				// creating the query term, and you should check to see whether
				// the token specifies a particular field (e.g., apple.title).
				int index = token.lastIndexOf('.');
				if (index != -1 && token.substring(index + 1).length() > 0) {
					String field = token.substring(index + 1);
					token = token.substring(0, index);
					currentOp.add(new QryopIlTerm(tokenizeQuery(token)[0],
							field));

				} else {
					String[] tokenized = tokenizeQuery(token);
					/*
					 * For words such as "in the", ignore them
					 */
					if (tokenized.length > 0) {
						currentOp.add(new QryopIlTerm(tokenizeQuery(token)[0]));
					}
				}
			}
		}

		// A broken structured query can leave unprocessed tokens on the
		// stack, so check for that.

		if (tokens.hasMoreTokens()) {
			System.err
					.println("Error:  Query syntax is incorrect.  " + qString);
			return null;
		}

		return currentOp;
	}

	public static List<String> outputEntry(String query, RetrievalModel r) {
		List<String> entries = new ArrayList<String>();

		String[] s = query.split(":");
		try {
			QryResult result = parseQuery(s[1]).evaluate(r);
			Collections
					.sort(result.docScores.scores, new ScoreListComparator());

			if (result.docScores.scores.size() == 0) {
				StringBuilder entry = new StringBuilder();
				entry.append(s[0]);
				entry.append(' ');
				entry.append("Q0");
				entry.append(' ');
				entry.append("dummy");
				entry.append(' ');
				entry.append("1" + ' ' + '0' + ' ' + "Run_1");

				entries.add(entry.toString());
			}
			for (int i = 0; i < Math.min(result.docScores.scores.size(), 100); i++) {
				StringBuilder entry = new StringBuilder();
				entry.append(s[0]);
				entry.append(' ');
				entry.append("Q0");
				entry.append(' ');
				entry.append(getExternalDocid(result.docScores.getDocid(i)));
				entry.append(' ');
				entry.append(String.valueOf(i + 1));
				entry.append(' ');
				entry.append(result.docScores.getDocidScore(i));
				entry.append(' ');
				entry.append("Run_1");

				entries.add(entry.toString());

			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return entries;

	}

	/**
	 * Print a message indicating the amount of memory used. The caller can
	 * indicate whether garbage collection should be performed, which slows the
	 * program but reduces memory usage.
	 * 
	 * @param gc
	 *            If true, run the garbage collector before reporting.
	 * @return void
	 */
	public static void printMemoryUsage(boolean gc) {

		Runtime runtime = Runtime.getRuntime();

		if (gc) {
			runtime.gc();
		}

		System.out
				.println("Memory used:  "
						+ ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L))
						+ " MB");
	}

	/**
	 * Print the query results.
	 * 
	 * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO
	 * THAT IT OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
	 * 
	 * QueryID Q0 DocID Rank Score RunID
	 * 
	 * @param queryName
	 *            Original query.
	 * @param result
	 *            Result object generated by {@link Qryop#evaluate()}.
	 * @throws IOException
	 */
	static void printResults(String queryName, QryResult result)
			throws IOException {

		System.out.println(queryName + ":  ");
		if (result.docScores.scores.size() < 1) {
			System.out.println("\tNo results.");
		} else {
			for (int i = 0; i < result.docScores.scores.size(); i++) {
				System.out.println("\t" + i + ":  "
						+ getExternalDocid(result.docScores.getDocid(i)) + ", "
						+ result.docScores.getDocidScore(i));
			}
		}
	}

	/**
	 * Given a query string, returns the terms one at a time with stopwords
	 * removed and the terms stemmed using the Krovetz stemmer.
	 * 
	 * Use this method to process raw query terms.
	 * 
	 * @param query
	 *            String containing query
	 * @return Array of query tokens
	 * @throws IOException
	 */
	static String[] tokenizeQuery(String query) throws IOException {

		TokenStreamComponents comp = analyzer.createComponents("dummy",
				new StringReader(query));
		TokenStream tokenStream = comp.getTokenStream();

		CharTermAttribute charTermAttribute = tokenStream
				.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		List<String> tokens = new ArrayList<String>();
		while (tokenStream.incrementToken()) {
			String term = charTermAttribute.toString();
			tokens.add(term);
		}
		return tokens.toArray(new String[tokens.size()]);
	}

	public static class ScoreListComparator implements
			Comparator<ScoreList.ScoreListEntry> {

		@Override
		public int compare(ScoreList.ScoreListEntry arg0,
				ScoreList.ScoreListEntry arg1) {
			if (arg0.score - arg1.score != 0) {
				return (int) (arg1.score - arg0.score);
			} else {
				try {
					return getExternalDocid(arg0.docid).compareTo(
							getExternalDocid(arg1.docid));
				} catch (IOException e) {
					return 0;
				}
			}
		}

	}
}
