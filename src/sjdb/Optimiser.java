package sjdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Optimiser implements PlanVisitor{
    //String startQuery = "PROJECT [projname,deptname] (SELECT [persname=\"Smith\"] (SELECT [dept=deptid] (SELECT [persid=manager] (((Person) TIMES (Project)) TIMES (Department)))))";
    //String endQuery = "PROJECT [projname,deptname] ((PROJECT [deptname,deptid] ((PROJECT [persid] (SELECT [persname="Smith"] (Person))) JOIN [persid,manager] (PROJECT [deptname, manager, deptid] (Department)))) JOIN [dept=deptid] (PROJECT [projname,dept] (Project)))"

    //Step 1.1 Parse through query with PlanVisitor
    //Step 1.2 Collect all Relations on scans
    //Step 1.3 Collect all Attributes that are needed for any operation and count number of times they are needed
    //Step 1.4 Collect all Predicates that are needed for any operation and count number of times they are needed
    //Step 2.1 Apply any Selects that can be done on a single relation to all relations collected during Scan
    //Step 2.2 Apply any Projects that can be done on a single relation to all relations collected during Scan
    //Step 2.3 Collect all new Operations "X * (Project(Select(Relation)))" into a list (oldList)
    //Step 3.1 Iterate through the List of operations until 1 operation remains in the list (while loop)
    //Step 3.2 Iterate through all operations(predicates) and join them if select (attribute = attribute) exists with attributes 1 attribute on 1 relation and the other attribute on the other relation (Another loop within loop within previous loop)
        //Form: Project((Project(Select(Relation))) JOIN [predicate] (Project(Select(Relation))))
        //If no selects remain (1 predicate remains) use Product
    //Step 3.3 Decrease attribute and predicate count or remove if value count at 1, then apply any projects that can be applied
    //Step 3.4 Add new Joined & Projected operations to new list of operations (newList)
    //Step 3.5 oldList = newList and repeat from 3.1 until 1 operation remains
    //Step 4 Return operation as new operation plan
    private Catalogue cat;
    private HashMap<Attribute, Integer> attributesNeededAndCount = new HashMap<>();
    private HashMap<Predicate, Integer> predicatesNeededAndCount = new HashMap<>();
    private ArrayList<Attribute> finalProject = new ArrayList<>();
    private ArrayList<Scan> relations = new ArrayList<>();
    private Estimator estimator = new Estimator();
    private boolean anyProjectsPresent = false;

    public Optimiser(Catalogue catalogue){
        cat = catalogue;
    }

    public Operator optimise(Operator op){
        // Step 1.1 Parse through query with PlanVisitor
        op.accept(this);
        // Step 2.3 Collect all new Operations "X * (Project(Select(Relation)))" into a list (oldList)
        List<Operator> trimmedRelations = initialTrimming();
        // Step 3
        Operator optimisedQuery = createOptimisedQuery(trimmedRelations);
        return optimisedQuery;
    }

    private Operator createOptimisedQuery(List<Operator> relations) {
        Operator optimisedPlan;
        List<Operator> remainingSubtrees = relations;

        // Loop until we have only one subtree left
        while (remainingSubtrees.size() > 1){
            boolean joinFound = false;

            // Try to find a join
            for (int i = 0; i < relations.size(); i++) {
                Operator relation1 = relations.get(i);
                if(relation1.getOutput() == null) {
                    relation1.accept(estimator);
                }
                Iterator<Predicate> predicateIterator = predicatesNeededAndCount.keySet().iterator();
                while (predicateIterator.hasNext()) {
                    Predicate predicate = predicateIterator.next();
                    if (relation1.getOutput().getAttributes().contains(predicate.getLeftAttribute())) {
                        for (int j = 0; j < relations.size(); j++) {
                            Operator relation2 = relations.get(j);
                            if(relation2.getOutput() == null) {
                                relation2.accept(estimator);
                            }
                            if (i != j){
                                if (relation2.getOutput().getAttributes().contains(predicate.getRightAttribute())) {
                                    // join the two relations using the predicate
                                    Operator newJoin = new Join(relation1, relation2, predicate);
                                    remainingSubtrees.add(newJoin);
                                    remainingSubtrees.remove(relation1);
                                    remainingSubtrees.remove(relation2);
                                    predicatesNeededAndCount.put(predicate, predicatesNeededAndCount.get(predicate) - 1);
                                    if (predicatesNeededAndCount.get(predicate) == 0){
                                        predicateIterator.remove();
                                    }
                                    attributesNeededAndCount.put(predicate.getLeftAttribute(), attributesNeededAndCount.get(predicate.getLeftAttribute()) - 1);
                                    if (attributesNeededAndCount.get(predicate.getLeftAttribute()) == 0) {
                                        attributesNeededAndCount.remove(predicate.getLeftAttribute());
                                    }
                                    attributesNeededAndCount.put(predicate.getRightAttribute(), attributesNeededAndCount.get(predicate.getRightAttribute()) - 1);
                                    if (attributesNeededAndCount.get(predicate.getRightAttribute()) == 0) {
                                        attributesNeededAndCount.remove(predicate.getRightAttribute());
                                    }

                                    if (anyProjectsPresent){
                                        if(newJoin.getOutput() == null) {
                                            newJoin.accept(estimator);
                                        }
                                        List<Attribute> relationAttributes = newJoin.getOutput().getAttributes();
                                        List<Attribute> attributesToProject = new ArrayList<>();
                                        for (Attribute attribute : attributesNeededAndCount.keySet()) {
                                            if (relationAttributes.contains(attribute)) {
                                                attributesToProject.add(attribute);
                                            }
                                        }
                                        if ((attributesToProject.size()) > 0 && (attributesToProject.size() < relationAttributes.size())) {
                                            Operator newRelation = new Project(newJoin, attributesToProject);
                                            remainingSubtrees.add(newRelation);
                                            remainingSubtrees.remove(newJoin);
                                        }
                                    }

                                    i--;
                                    joinFound = true;
                                    break;
                                }
                            }
                        }
                        if (joinFound) {
                            break;
                        }
                    }
                }
                if (joinFound) {
                    break;
                }
            }
            // If we didn't find a join, use a Cartesian product or cross join
            if (!joinFound) {
                Operator newJoin = new Product(remainingSubtrees.get(0), remainingSubtrees.get(1));
                remainingSubtrees.remove(0);
                remainingSubtrees.remove(0);
                remainingSubtrees.add(newJoin);
            }
        }
        optimisedPlan = remainingSubtrees.get(0);
        return optimisedPlan;
    }

    //Step 2.3 Collect all new Operations "X * (Project(Select(Relation)))" into a list (oldList)
    private List<Operator> initialTrimming() {
        List<Operator> oldList = new ArrayList<>(relations.size());
        // Step 2.1 Apply any Selects that can be done on a single relation to all relations collected during Scan
        for (Operator relation : relations) {
            boolean selectApplied = false;
            Iterator<Predicate> predicateIterator = predicatesNeededAndCount.keySet().iterator();
            while (predicateIterator.hasNext()) {
                Predicate predicate = predicateIterator.next();
                if (canApplySelectOnRelation(relation, predicate)) {
                    Operator trimmedRelation = new Select(relation, predicate);
                    oldList.add(trimmedRelation);
                    selectApplied = true;
                    predicatesNeededAndCount.put(predicate, predicatesNeededAndCount.get(predicate) - 1);
                    if (predicatesNeededAndCount.get(predicate) == 0){
                        predicateIterator.remove();
                    }
                    attributesNeededAndCount.put(predicate.getLeftAttribute(), attributesNeededAndCount.get(predicate.getLeftAttribute()) - 1);
                    if (attributesNeededAndCount.get(predicate.getLeftAttribute()) == 0) {
                        attributesNeededAndCount.remove(predicate.getLeftAttribute());
                    }
                    if(!predicate.equalsValue()) {
                        attributesNeededAndCount.put(predicate.getRightAttribute(), attributesNeededAndCount.get(predicate.getRightAttribute()) - 1);
                        if (attributesNeededAndCount.get(predicate.getRightAttribute()) == 0) {
                            attributesNeededAndCount.remove(predicate.getRightAttribute());
                        }
                    }
                }
            }  if (!selectApplied) {
                oldList.add(relation);
            }
        }
        // Step 2.2 Apply any Projects that can be done on a single relation to all relations collected during Scan
        List<Operator> newList = new ArrayList<>();
        if (attributesNeededAndCount.isEmpty()) {
            return oldList;
        }
        if (!anyProjectsPresent){
            return oldList;
        }
        for (Operator relation : oldList) {
            if(relation.getOutput() == null) {
                relation.accept(estimator);
            }
            List<Attribute> relationAttributes = relation.getOutput().getAttributes();
            List<Attribute> attributesToProject = new ArrayList<>();
            for (Attribute attribute : attributesNeededAndCount.keySet()) {
                if (relationAttributes.contains(attribute)) {
                    attributesToProject.add(attribute);
                }
            }
            if (attributesToProject.size() > 0) {
                relation = new Project(relation, attributesToProject);
                newList.add(relation);
            }
        }
        return newList;
    }

    //Helper for initialTrimming()
    private boolean canApplySelectOnRelation(Operator relation, Predicate predicate) {
        Attribute leftAttribute = predicate.getLeftAttribute();
        if (!relation.getOutput().getAttributes().contains(leftAttribute)) {
            return false;
        }
        if (predicate.equalsValue()) {
            return true;
        }
        Attribute rightAttribute = predicate.getRightAttribute();
        return relation.getOutput().getAttributes().contains(rightAttribute);
    }

    // Step 1.2 Collect all Relations on scans
    @Override
    public void visit(Scan op) {
        relations.add(new Scan((NamedRelation)op.getRelation()));
    }
    // Step 1.3.1 Collect all Attributes from Projects
    @Override
    public void visit(Project op) {
        List<Attribute> attributes = op.getAttributes();
        for (Attribute attribute : attributes) {
            int count = attributesNeededAndCount.getOrDefault(attribute, 0);
            attributesNeededAndCount.put(attribute, count + 1);
            finalProject.add(attribute);
        }
        anyProjectsPresent = true;

    }
    // Step 1.3.2 Collect all Attributes from Selects
    // Step 1.4 Collect all Predicates from Selects
    @Override
    public void visit(Select op) {
        Predicate predicate = op.getPredicate();

        Attribute leftAttribute = predicate.getLeftAttribute();
        int leftCount = attributesNeededAndCount.getOrDefault(leftAttribute, 0);
        attributesNeededAndCount.put(leftAttribute, leftCount + 1);

        if (!predicate.equalsValue()) {
            Attribute rightAttribute = predicate.getRightAttribute();
            int rightCount = attributesNeededAndCount.getOrDefault(rightAttribute, 0);
            attributesNeededAndCount.put(rightAttribute, rightCount + 1);
        }

        int predicateCount = predicatesNeededAndCount.getOrDefault(predicate, 0);
        predicatesNeededAndCount.put(predicate, predicateCount + 1);
    }

    @Override
    public void visit(Product op) {
        String productTemp = "Product";
    }
    @Override
    public void visit(Join op) {
        Predicate predicate = op.getPredicate();

        Attribute leftAttribute = predicate.getLeftAttribute();
        int leftCount = attributesNeededAndCount.getOrDefault(leftAttribute, 0);
        attributesNeededAndCount.put(leftAttribute, leftCount + 1);

        if (!predicate.equalsValue()) {
            Attribute rightAttribute = predicate.getRightAttribute();
            int rightCount = attributesNeededAndCount.getOrDefault(rightAttribute, 0);
            attributesNeededAndCount.put(rightAttribute, rightCount + 1);
        }

        int predicateCount = predicatesNeededAndCount.getOrDefault(predicate, 0);
        predicatesNeededAndCount.put(predicate, predicateCount + 1);
    }

}
