package simpledb;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * TDItem记录了每个field的类型和名称
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        public final Type fieldType;//类型

        public final String fieldName;//名称

        //构造函数
        public TDItem(Type t, String n) {
            this.fieldType = t;
            this.fieldName = n;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }//end of TDItem class

    /**
     * 迭代器
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return desc.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public ArrayList<TDItem> desc;
    public Type[] typeArr;
    public String[] fieldArr;

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        desc = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {//循环遍历，向desc添加TDItem对象
            desc.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        this.typeArr = typeAr;
        this.fieldArr = fieldAr;
    }

    public TupleDesc(ArrayList<TDItem> desc) {
        this.desc = desc;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {//所有field名称都为匿名
        // some code goes here
        desc = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            desc.add(new TDItem(typeAr[i], null));
        }
        this.typeArr = typeAr;
        this.fieldArr = null;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return desc.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return desc.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return desc.get(i).fieldType;
    }

    public Type[] getFieldTypeArr() {
        Type[] type = new Type[numFields()];
        for (int i = 0; i < numFields(); i++) {
            type[i] = desc.get(i).fieldType;
        }
        return type;
    }

    public String[] getFieldNameArr() {
        String[] name = new String[numFields()];
        for (int i = 0; i < numFields(); i++) {
            name[i] = desc.get(i).fieldName;
        }
        return name;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) {
        if (name == null) throw new NoSuchElementException("name cannot be null");
        //利用迭代器循环遍历
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {
            TDItem t = it.next();
            try {//有可能所有field都是匿名：抛出异常
                if (t.fieldName.equals(name)) return desc.indexOf(t);
            } catch (NullPointerException e) {
                throw new NoSuchElementException("no fields are named");
            }
        }
        //找不到对应field：抛出异常
        throw new NoSuchElementException("no item has been found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {//很好理解
            size += it.next().fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge1(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> merge = new ArrayList<TDItem>(td2.desc);
        merge.addAll(td1.desc);
        return new TupleDesc(merge);
    }
    public static TupleDesc merge2(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] mergeType = new Type[td1.typeArr.length + td2.typeArr.length];
        String[] mergeName = new String[td1.fieldArr.length + td2.fieldArr.length];
        //合并类型
        for (int i = 0; i < mergeType.length; i++) {
            if (i < td1.typeArr.length) {
                mergeType[i] = td1.typeArr[i];
            } else {
                mergeType[i] = td2.typeArr[i - td1.typeArr.length];
            }
        }
        //合并名称
        for (int i = 0; i < mergeName.length; i++) {
            if (i < td1.fieldArr.length) {
                mergeName[i] = td1.fieldArr[i];
            } else {
                mergeName[i] = td2.fieldArr[i - td1.fieldArr.length];
            }
        }
        return new TupleDesc(mergeType, mergeName);
    }

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] fields = new String[td1.numFields() + td2.numFields()];
        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            fields[i] = td1.getFieldName(i);
        }
        int offset = td1.numFields();
        for (int i = 0; i < td2.numFields(); i++) {
            types[offset + i] = td2.getFieldType(i);
            fields[offset + i] = td2.getFieldName(i);
        }
        return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        //对比自己
        if (o == this) {
            return true;
        }
        //判断类型
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc t = (TupleDesc) o;
        if (this.typeArr.length == t.typeArr.length) {
            for (int i = 0; i < this.typeArr.length; i++) {
                if (this.typeArr[i] != t.typeArr[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return desc.toString();
    }
}
