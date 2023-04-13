package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class Test {
	private Catalogue catalogue;
	
	public Test() {
	}

	public static void main(String[] args) throws Exception {
		Catalogue catalogue = createCatalogue2();
		Inspector inspector = new Inspector();
		Estimator estimator = new Estimator();
		Operator plan = query12(catalogue);

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  Original Plan Cost  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		plan.accept(estimator);
		plan.accept(inspector);
		System.out.println("\nTotal Cost: " + estimator.getTotalCost());
		//System.out.println("\nTotal Cost: " + estimator.getCost(plan));

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  Optimised Plan Cost  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		Optimiser optimiser = new Optimiser(catalogue);
		Operator planopt = optimiser.optimise(plan);
		planopt.accept(estimator);
		planopt.accept(inspector);
		System.out.println("\nTotal Cost: " + estimator.getCost(planopt));
	}
	
	public static Catalogue createCatalogue() {
		Catalogue cat = new Catalogue();
		cat.createRelation("A", 100);
		cat.createAttribute("A", "a1", 100);
		cat.createAttribute("A", "a2", 15);
		cat.createRelation("B", 150);
		cat.createAttribute("B", "b1", 150);
		cat.createAttribute("B", "b2", 100);
		cat.createAttribute("B", "b3", 5);
		
		return cat;
	}

	public static Operator query(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("A"));
		Scan b = new Scan(cat.getRelation("B")); 
		
		Product p1 = new Product(a, b);
		
		Select s1 = new Select(p1, new Predicate(new Attribute("a2"), new Attribute("b3")));
		
		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("a2"));	
		atts.add(new Attribute("b1"));	
		
		Project plan = new Project(s1, atts);
		
		return plan;
	}

	public static Catalogue createCatalogue2() {
		Catalogue cat = new Catalogue();
		cat.createRelation("Person", 400);
		cat.createAttribute("Person", "persid", 400);
		cat.createAttribute("Person", "persname", 350);
		cat.createAttribute("Person", "age", 47);
		cat.createRelation("Project", 40);
		cat.createAttribute("Project", "projid", 40);
		cat.createAttribute("Project", "projname", 35);
		cat.createAttribute("Project", "dept", 5);
		cat.createRelation("Department", 5);
		cat.createAttribute("Department", "deptid", 5);
		cat.createAttribute("Department", "deptname", 5);
		cat.createAttribute("Department", "manager", 5);
		return cat;
	}

	//Scan
	public static Operator query1(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		return a;
	}

	//Select + Scan
	public static Operator query2(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Select b = new Select(a, new Predicate(new Attribute("age"), "35"));
		return b;
	}

	//Project + Scan
	public static Operator query3(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("persname"));

		Project c = new Project(a, atts);
		return c;
	}

	//Project + Select + Scan
	public static Operator query4(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Select b = new Select(a, new Predicate(new Attribute("age"), "35"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("persname"));

		Project c = new Project(b, atts);
		return c;
	}

	//Product + Scan
	public static Operator query5(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Project"));
		Scan b = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, b, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product c = new Product(a, b);
		return c;
	}

	//Product + Select + Scan
	public static Operator query6(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Project"));
		Scan b = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, b, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product c = new Product(a, b);
		Select d = new Select(c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		return d;
	}

	//Product + Project + Scan
	public static Operator query7(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Project"));
		Scan b = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, b, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product c = new Product(a, b);

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projid"));
		atts.add(new Attribute("manager"));

		Project f = new Project(c, atts);
		return f;
	}

	//Product + Project + Select + Scan
	public static Operator query8(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Project"));
		Scan b = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, b, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product c = new Product(a, b);
		Select d = new Select(c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Select e = new Select(d, new Predicate(new Attribute("deptname"), "Research"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projid"));

		Project f = new Project(e, atts);
		return f;
	}

	//Product + Project + Select + Scan (2)
	public static Operator query9(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Scan b = new Scan(cat.getRelation("Project"));
		Scan c = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, c, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join e = new Join(b, d, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join d = new Join(b, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join e = new Join(a, d, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join d = new Join(a, b, new Predicate(new Attribute("persid"), new Attribute("dept")));
		//Join e = new Join(d, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product d = new Product(a, b);
		Product dd = new Product(d, c);
		Select ee = new Select(dd, new Predicate(new Attribute("persid"), new Attribute("manager")));
		Select e = new Select(ee, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Select f = new Select(e, new Predicate(new Attribute("persname"), "Smith"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projname"));
		atts.add(new Attribute("deptname"));

		Project g = new Project(f, atts);
		return g;
	}

	//Attribute needed for multiple selects + Reverse order
	public static Operator query10(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Scan b = new Scan(cat.getRelation("Project"));
		Scan c = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, c, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join e = new Join(b, d, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join d = new Join(b, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join e = new Join(a, d, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join d = new Join(a, b, new Predicate(new Attribute("persid"), new Attribute("dept")));
		//Join e = new Join(d, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product d = new Product(a, b);
		Product dd = new Product(d, c);
		Select ee = new Select(dd, new Predicate(new Attribute("persid"), new Attribute("manager")));
		Select e = new Select(ee, new Predicate(new Attribute("persid"), new Attribute("dept")));
		Select f = new Select(e, new Predicate(new Attribute("persname"), "Smith"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projname"));
		atts.add(new Attribute("deptname"));

		Project g = new Project(f, atts);
		return g;
	}

	//Select + Product != Join
	public static Operator query11(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Scan b = new Scan(cat.getRelation("Project"));
		Scan c = new Scan(cat.getRelation("Department"));
		//Join d = new Join(a, c, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join e = new Join(b, d, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join d = new Join(b, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join e = new Join(a, d, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join d = new Join(a, b, new Predicate(new Attribute("persid"), new Attribute("dept")));
		//Join e = new Join(d, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Product d = new Product(a, b);
		Product dd = new Product(d, c);
		Select ee = new Select(dd, new Predicate(new Attribute("persid"), new Attribute("manager")));
		Select f = new Select(ee, new Predicate(new Attribute("persname"), "Smith"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projname"));
		atts.add(new Attribute("deptname"));

		Project g = new Project(f, atts);
		return g;
	}

	//Joins in Query
	public static Operator query12(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("Person"));
		Scan b = new Scan(cat.getRelation("Project"));
		Scan c = new Scan(cat.getRelation("Department"));
		Join d = new Join(a, c, new Predicate(new Attribute("persid"), new Attribute("manager")));
		Join e = new Join(b, d, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join d = new Join(b, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Join e = new Join(a, d, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Join d = new Join(a, b, new Predicate(new Attribute("persid"), new Attribute("dept")));
		//Join e = new Join(d, c, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		//Product d = new Product(a, b);
		//Product dd = new Product(d, c);
		//Select ee = new Select(dd, new Predicate(new Attribute("persid"), new Attribute("manager")));
		//Select e = new Select(d, new Predicate(new Attribute("dept"), new Attribute("deptid")));
		Select f = new Select(e, new Predicate(new Attribute("persname"), "Smith"));

		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("projname"));
		atts.add(new Attribute("deptname"));

		Project g = new Project(f, atts);
		return g;
	}
	//TODO: query12: joins already exist in original query
	
}

