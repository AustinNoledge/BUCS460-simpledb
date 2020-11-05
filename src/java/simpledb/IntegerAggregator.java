package simpledb;

import com.sun.deploy.security.ValidationState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex, aggFieldIndex;
    private Type gbFieldType;
    private Op aggOperator;
    private Map<Field, Integer> gbMap;
    private Map<Field, ArrayList<Integer>> gbAvgMap;
    private int noGroupResult, noGroupCount;
    private boolean noGroup = false;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.aggOperator = what;
        if (this.aggOperator == Op.AVG) {
            this.gbAvgMap = new HashMap<>();
        } else {
            this.gbMap = new HashMap<Field, Integer>();
        }
        if (gbfield == Aggregator.NO_GROUPING) {
            this.noGroupResult = 0;
            this.noGroupCount = 0;
            this.noGroup = true;
        }
    }

    private void noGroupVersionMerge(Tuple tup) {
        Integer aggCol = (Integer) ((IntField) tup.getField(this.aggFieldIndex)).getValue();
        boolean firstElement = (this.noGroupCount==0) ? true : false;
        if (firstElement) {
            this.noGroupCount++;
            this.noGroupResult = aggCol;
        } else {
            this.noGroupCount++;
            switch (this.aggOperator) {
                case SUM:
                    this.noGroupResult += aggCol;
                    break;

                case AVG:
                    this.noGroupResult += aggCol;
                    // 记得最后要除count
                    break;

                case MAX:
                    if (aggCol > noGroupResult)
                        noGroupResult = aggCol;
                    break;

                case MIN:
                    if (aggCol < noGroupResult)
                        noGroupResult = aggCol;
                    break;

                case COUNT:
                    break;

                default:
                    break;
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (noGroup) {
            noGroupVersionMerge(tup);
        } else {
            Field gbCol = tup.getField(this.gbFieldIndex);
            Integer aggCol = (Integer) ((IntField) tup.getField(this.aggFieldIndex)).getValue();
            boolean firstElement;
            if (this.aggOperator != Op.AVG) {
                firstElement = (gbMap.containsKey(gbCol)) ? false : true;
            } else {
                firstElement = (gbAvgMap.containsKey(gbCol)) ? false : true;
            }

            if (firstElement && !this.aggOperator.equals(Op.COUNT) && !this.aggOperator.equals(Op.AVG)) {
                gbMap.put(gbCol, aggCol);
            } else if (firstElement && this.aggOperator.equals(Op.COUNT)) {
                gbMap.put(gbCol, 1);
            } else if (firstElement && this.aggOperator.equals(Op.AVG)) {
                ArrayList<Integer> countAndSum = new ArrayList<>();
                countAndSum.add(1);
                countAndSum.add(aggCol);
                gbAvgMap.put(gbCol, countAndSum);
            } else {
                Integer oldValue = null;
                ArrayList<Integer> oldAmountAndResult = null;
                if (this.aggOperator == Op.AVG) {
                    oldAmountAndResult = gbAvgMap.get(gbCol);
                } else {
                    oldValue = gbMap.get(gbCol);
                }
                switch (this.aggOperator) {
                    case SUM:
                        gbMap.replace(gbCol, oldValue + aggCol);
                        break;

                    case AVG:
                        Integer oldCount = oldAmountAndResult.get(0);
                        Integer oldSum = oldAmountAndResult.get(1);
                        oldAmountAndResult.set(0, oldCount + 1);
                        oldAmountAndResult.set(1, oldSum + aggCol);
                        break;

                    case MAX:
                        if (aggCol > oldValue) {
                            gbMap.replace(gbCol, aggCol);
                        }
                        break;

                    case MIN:
                        if (aggCol < oldValue) {
                            gbMap.replace(gbCol, aggCol);
                        }
                        break;

                    case COUNT:
                        oldValue++;
                        gbMap.replace(gbCol, oldValue);
                        break;

                    default:
                        try {
                            throw new DbException("We don't support this operator yet");
                        } catch (DbException e) {
                            e.printStackTrace();
                        }
                }
            }
        }
    }

    private TupleDesc getTd() {
        // some code goes here
        Type[] typeArr;
        String[] nameArr;

        if (this.gbFieldIndex == Aggregator.NO_GROUPING) {
            typeArr = new Type[1];
            nameArr = new String[1];
            typeArr[0] = Type.INT_TYPE;
            nameArr[0] = "";
        } else {
            typeArr = new Type[2];
            nameArr = new String[2];
            typeArr[0] = this.gbFieldType;
            nameArr[0] = "";
            typeArr[1] = Type.INT_TYPE;
            nameArr[1] = "";
        }

        return new TupleDesc(typeArr, nameArr);
    }

    private DbIterator noGroupVersionIterator() {
        TupleDesc td = this.getTd();
        ArrayList<Tuple> tuples = new ArrayList<>();
        Field value;
        if (this.aggOperator != Op.AVG) {
            value = new IntField((Integer) this.noGroupResult);
        } else {
            value = new IntField((Integer) this.noGroupResult / this.noGroupCount);
        }
        Tuple tuple = new Tuple(td);
        tuple.setField(0, value);
        tuples.add(tuple);
        return new TupleIterator(td, tuples);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if (this.noGroup) {
            return noGroupVersionIterator();
        }
        TupleDesc td = this.getTd();
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Iterator mapIt;
        mapIt = (this.aggOperator == Op.AVG) ? this.gbAvgMap.entrySet().iterator() :
                this.gbMap.entrySet().iterator();
        while (mapIt.hasNext()) {
            Map.Entry mapElement = (Map.Entry) mapIt.next();
            Field key = (Field) mapElement.getKey();
            Field value;
            if (this.aggOperator != Op.AVG) {
                value = new IntField((Integer) mapElement.getValue());
            } else {
                ArrayList<Integer> ar = (ArrayList) mapElement.getValue();
                value = new IntField((Integer) (ar.get(1) / ar.get(0)));
            }
            Tuple tuple = new Tuple(td);
            tuple.setField(0, key);
            tuple.setField(1, value);
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
