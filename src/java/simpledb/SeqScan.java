package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias（关于每个field的更加可读的名称）
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    //测试阶段：不知道需要什么field（全部声明）
    public TransactionId tid;
    public int tableid;
    public String tableAlias;
    public DbFile table;
    public DbFileIterator it = null;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.table = Database.getCatalog().getDatabaseFile(tableid);
        this.it = table.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    //默认tableAlias的constructor
    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    //TupleDesc的constructor：TupleDesc(Type[] typeAr, String[] fieldAr)
    //SeqScan的constructor：SeqScan(TransactionId tid, int tableid, String tableAlias)
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc desc = this.table.getTupleDesc();//根据DbFile获取TupleDesc
        Type[] typeArr = desc.getFieldTypeArr();//直接获取Type数组

        String[] nameArr = desc.getFieldNameArr().clone();//直接复制Nanme数组，然后每个元素添加alias.
        for (int i = 0; i < desc.numFields(); i++) {
            nameArr[i] = getAlias() + "." + nameArr[i];
        }
        return new TupleDesc(typeArr, nameArr);
    }

    //这里开始，所有iterator相关函数全部直接使用HeapFile的
    //理由，两者目的相同：都是遍历HeapFile内所有tuples
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        it.open();
    }
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return it.next();
    }

    public void close() {
        // some code goes here
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        it.rewind();
    }
}
