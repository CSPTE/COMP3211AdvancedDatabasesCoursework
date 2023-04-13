package sjdb;

import java.util.Iterator;

public class Estimator implements PlanVisitor {
	private int totalCost = 0;

	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);

		totalCost = totalCost + output.getTupleCount();
	}

	public void visit(Project op) {
		//Operator intermediary = op.getInput();
		//Relation intermediary2  = op.getOutput();
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());

		for(Attribute attributesKept : op.getAttributes()){
			for (Attribute allAttributes : input.getAttributes()) {
				if (attributesKept.equals(allAttributes)) {
					output.addAttribute(new Attribute(allAttributes));
				}
			}
		}

		op.setOutput(output);

		totalCost = totalCost + output.getTupleCount();
	}
	
	public void visit(Product op) {
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();

		Relation output = new Relation(leftInput.getTupleCount() * rightInput.getTupleCount());

		for (Attribute attrLeft : leftInput.getAttributes()){
			output.addAttribute(new Attribute(attrLeft));
		}
		for (Attribute attrRight : rightInput.getAttributes()){
			output.addAttribute(new Attribute(attrRight));
		}

		op.setOutput(output);

		totalCost = totalCost + output.getTupleCount();
	}

	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Relation output;

		Predicate predicate = op.getPredicate();
		Attribute predicateLeftAttribute = predicate.getLeftAttribute();
		Attribute predicateRightAttribute = predicate.getRightAttribute();

		if(predicate.equalsValue()) {
			Attribute inputLeftAttribute = input.getAttribute(predicateLeftAttribute);
			int toupleCount = Math.round((float) input.getTupleCount()/inputLeftAttribute.getValueCount());
			output = new Relation(toupleCount);

			for (Attribute attr : input.getAttributes()) {
				if (attr.equals(inputLeftAttribute)) {
					output.addAttribute(new Attribute(attr.getName(), 1));
				} else {
					output.addAttribute(new Attribute(attr));
				}
			}
		} else {
			Attribute inputLeftAttribute = input.getAttribute(predicateLeftAttribute);
			Attribute inputRightAttribute = input.getAttribute(predicateRightAttribute);

			int maxCount = Math.max(inputLeftAttribute.getValueCount(),inputRightAttribute.getValueCount());
			int minCount = Math.min(inputLeftAttribute.getValueCount(),inputRightAttribute.getValueCount());

			output = new Relation(input.getTupleCount()/maxCount);

			for (Attribute attr : input.getAttributes()) {
				if (attr.equals(inputLeftAttribute) || (attr.equals(inputRightAttribute))) {
					output.addAttribute(new Attribute(attr.getName(), minCount));
				} else {
					output.addAttribute(new Attribute(attr));
				}
			}
		}

		op.setOutput(output);

		totalCost = totalCost + output.getTupleCount();
	}
	
	public void visit(Join op) {
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();

		Predicate predicate = op.getPredicate();
		Attribute predicateLeftAttribute = predicate.getLeftAttribute();
		Attribute predicateRightAttribute = predicate.getRightAttribute();

		Attribute inputLeftAttribute = leftInput.getAttribute(predicateLeftAttribute);
		Attribute inputRightAttribute = rightInput.getAttribute(predicateRightAttribute);

		int maxCount = Math.max(inputLeftAttribute.getValueCount(), inputRightAttribute.getValueCount());
		int minCount = Math.min(inputLeftAttribute.getValueCount(), inputRightAttribute.getValueCount());

		Relation output = new Relation((leftInput.getTupleCount() * rightInput.getTupleCount())/maxCount);

		for (Attribute attr : leftInput.getAttributes()) {
			if (attr.equals(inputLeftAttribute)) {
				output.addAttribute(new Attribute(attr.getName(), minCount));
			} else {
				output.addAttribute(new Attribute(attr));
			}
		}

		for (Attribute attr : rightInput.getAttributes()) {
			if (attr.equals(inputRightAttribute)) {
				output.addAttribute(new Attribute(attr.getName(), minCount));
			} else {
				output.addAttribute(new Attribute(attr));
			}
		}

		op.setOutput(output);

		totalCost = totalCost + output.getTupleCount();
	}

	public int getTotalCost(){
		return totalCost;
	}

	public int getCost(Operator plan) {
		this.totalCost = 0;
		plan.accept(this);
		return this.totalCost;
	}
}
