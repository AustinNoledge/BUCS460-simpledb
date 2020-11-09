package simpledb;

import java.math.BigInteger;
import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    TransactionId tid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;
    boolean isDirty;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        //读取header
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            //1 bit header说明数据是否有效
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        //读取tuples
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        // some code goes here
        //根据数据结构获得tuple大小（题目假设所有tuple大小一致）
        int tupleSize = this.td.getSize();
        return (int) Math.floor((BufferPool.getPageSize()*8)/(tupleSize*8+1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    public int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(this.numSlots/8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        boolean deleted = false;
        if (!this.pid.equals(t.getRecordId().getPageId()))
            throw new DbException("pid not matched");
        for (int i = 0; i < this.numSlots; i++) {
            int validBit = this.getValidBit(i);
            boolean usedornot = this.isSlotUsed(i);
            if (this.isSlotUsed(i)) {
                if (this.tuples[i].equals(t)) {
                    deleted = true;
                    this.tuples[i] = null;
                    this.markSlotUsed(i, false);
                    break;
                }
            }
        }
        if (!deleted)
            throw new DbException("cannot find the target tuple");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        boolean inserted = false;
        for (int i = 0; i < this.numSlots; i++) {
            if (!isSlotUsed(i)) {
                this.tuples[i] = t;
                t.setRecordId(new RecordId(this.getId(), i));
                markSlotUsed(i, true);
                inserted = true;
                break;
            }
        }
        if (!inserted)
            throw new DbException("No free space to insert on this page");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.isDirty = dirty;
        this.tid = dirty? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return isDirty? tid : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int numEmpty = 0;
        int headerBit = getHeaderSize() * 8;
        for (int i = 0; i < headerBit; i++) {
            if (getValidBit(i) == 0)
                numEmpty++;
        }
        return numEmpty;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    private int getValidBit(int i) {
        int byteIndex = i / 8;
        int bitIndex = i % 8;
        //需要将读取header bit的函数单独拆出，方便其他方法使用
        String validNum = String.format("%8s", Integer.toBinaryString(header[byteIndex]))
                .replace(' ', '0');
        if (i < 100)
            validNum = validNum.substring(validNum.length() - 8);
        return Integer.parseInt(String.
                valueOf(validNum.charAt(7-bitIndex)));
    }

    public boolean isSlotUsed(int i) {
        // some code goes here
        int validBit = getValidBit(i);
        if (validBit == 1)
            return true;
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int byteIndex = i / 8;
        int bitIndex = i % 8;
        if (value) {
            header[byteIndex] |= 1 << (bitIndex);
        } else {
            header[byteIndex] &= ~(1 << (bitIndex));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    //make an auxilary class for iterator
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageIterator();
    }
    //基本逻辑：
    //hasNext判断后面是否还有validBit为1的tuple
    //next移动到下一个validBit为1的tuple
    /**
    private class HeapPageIterator implements Iterator<Tuple> {
        private Tuple curr = null;
        private final int totalTuple = getNumTuples();
        private final int validTuple = getNumTuples()-getNumEmptySlots();//计算有多少个有效tuple
        private int visitedTuple = 0;//记录已经访问的tuple个数，等于validTuple时hasNext()返回false
        private int currPosition = -1;

        public boolean hasNext() {
            return (visitedTuple < validTuple);
        }

        public Tuple next() {
            if (!hasNext()) throw new NoSuchElementException();
            for (int i = currPosition+1; i < totalTuple; i++) {
                if (getValidBit(i) == 1) {//更新curr，currPosition，visitedTuple
                    currPosition = i;
                    visitedTuple++;
                    curr = tuples[i];
                    break;
                }
            }
            return curr;
        }
    }
     */
    private class HeapPageIterator implements Iterator<Tuple> {
        private int currPos;

        public HeapPageIterator() {
            currPos = 0;
        }

        public boolean hasNext() {
            for (int i = currPos; i < numSlots; i++) {
                if (isSlotUsed(i)) {
                    currPos = i;
                    return true;
                }
            }
            return false;
        }

        public Tuple next() {
            if (!hasNext()) throw new NoSuchElementException();
            Tuple t = tuples[currPos];
            currPos++;
            return t;
        }
    }
}

