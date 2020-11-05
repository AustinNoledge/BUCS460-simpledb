package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex, aggFieldIndex;
    private Type gbFieldType;
    private Op aggOperator;
    private Map<Field, Integer> gbMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.aggOperator = what;
        this.gbMap = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbCol = tup.getField(gbFieldIndex);
        Field aggCol = tup.getField(aggFieldIndex);
        if (!this.aggOperator.equals(Op.COUNT)) {
            try {
                throw new DbException("We don't support this operator for string yet");
            } catch (DbException e) {
                e.printStackTrace();
            }
        } else {
            gbMap.putIfAbsent(gbCol, 0);
            Integer oldValue = gbMap.get(gbCol);
            oldValue++;
            gbMap.replace(gbCol, oldValue);
        }
    }

    private TupleDesc getTd() {
        Type[] typeAr = new Type[2];
        typeAr[0] = gbFieldType;
        typeAr[1] = Type.INT_TYPE;
        return new TupleDesc(typeAr);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td = this.getTd();
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Iterator mapIt = this.gbMap.entrySet().iterator();
        while (mapIt.hasNext()) {
            Map.Entry mapElement = (Map.Entry) mapIt.next();
            Field key = (Field) mapElement.getKey();
            Field value = new IntField((Integer) mapElement.getValue());
            Tuple tuple = new Tuple(td);
            tuple.setField(0, key);
            tuple.setField(1, value);
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
