/**
 * 
 */
package sjdb;
import java.io.*;

/**
 * @author nmg
 *
 */
public class SJDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// read serialised catalogue from file and parse
		String catFile = "data/cat.txt";//args[0];
		Catalogue cat = new Catalogue();
		CatalogueParser catParser = new CatalogueParser(catFile, cat);
		catParser.parse();
		
		// read stdin, parse, and build canonical query plan
		//QueryParser queryParser = new QueryParser(cat, new InputStreamReader(System.in));
		QueryParser queryParser = new QueryParser(cat, new FileReader(new File("data/q5.txt")));
		Operator plan = queryParser.parse();
		System.out.println("Plan: " + plan.toString());
				
		// create estimator visitor and apply it to canonical plan
		Estimator est = new Estimator();
		plan.accept(est);
		//System.out.println("Total Cost: " + est.getTotalCost());
		System.out.println("Total Cost: " + est.getCost(plan));

		// create optimised plan
		Optimiser opt = new Optimiser(cat);
		Operator optPlan = opt.optimise(plan);
		System.out.println("\nOptimised Plan: " + optPlan.toString());
		System.out.println("Total Cost: " + est.getCost(optPlan));

		/*
		Estimator est2 = new Estimator();
		optPlan.accept(est2);
		System.out.println("Total Cost: " + est.getTotalCost());
		 */
	}

}
