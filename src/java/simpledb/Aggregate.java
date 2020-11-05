package simpledb;

import java.rmi.activation.ActivationGroup;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int aggFieldIndex, gbFieldIndex;
    private Aggregator.Op operator;
    private Type gbFieldType;
    private Type aggFieldType;
    private Aggregator aggregator;
    private DbIterator aggIt;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.aggFieldIndex = afield;
        this.gbFieldIndex = gfield;
        this.operator = aop;
        gbFieldType = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);
        aggFieldType = child.getTupleDesc().getFieldType(afield);
        if (aggFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        } else if (aggFieldType == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
        } else {
            try {
                throw new DbException("No such aggregation type");
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        //this.aggIt = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return this.gbFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (gbFieldIndex == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gbFieldIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return this.aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return child.getTupleDesc().getFieldName(this.aggFieldIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        this.child.open();
        super.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggIt = aggregator.iterator();
        aggIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (aggIt.hasNext()) {
            return aggIt.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        aggIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        Type[] typeArr;
        String[] nameArr;

        if (this.aggFieldIndex == Aggregator.NO_GROUPING) {
            typeArr = new Type[1];
            nameArr = new String[1];
            typeArr[0] = child.getTupleDesc().getFieldType(this.aggFieldIndex);
            nameArr[0] = this.operator.toString() + " (" + child.getTupleDesc().getFieldName(this.aggFieldIndex) + ")";
        } else {
            typeArr = new Type[2];
            nameArr = new String[2];
            typeArr[0] = child.getTupleDesc().getFieldType(this.gbFieldIndex);
            nameArr[0] = child.getTupleDesc().getFieldName(this.gbFieldIndex);
            typeArr[1] = child.getTupleDesc().getFieldType(this.aggFieldIndex);
            nameArr[1] = this.operator.toString() + " (" + child.getTupleDesc().getFieldName(this.aggFieldIndex) + ")";
        }

        return new TupleDesc(typeArr, nameArr);
    }

    public void close() {
	// some code goes here
        super.close();
        aggIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        this.child = children[0];
    }
    
}
