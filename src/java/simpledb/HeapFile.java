package simpledb;

import javax.xml.crypto.Data;
import java.awt.image.AreaAveragingScaleFilter;
import java.io.*;
import java.lang.reflect.Array;
import java.security.Permission;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public File file;
    public RandomAccessFile raf;
    public TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        try {
            this.raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        long offset = BufferPool.getPageSize() * pid.pageNumber();
        byte[] readInfo = new byte[BufferPool.getPageSize()];
        try {
            raf.seek(offset);
            raf.read(readInfo, 0, readInfo.length);
            HeapPageId hid = new HeapPageId(getId(), pid.pageNumber());
            Page pg = new HeapPage(hid, readInfo);
            return pg;
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        raf.seek(page.getId().pageNumber() * BufferPool.getPageSize());
        byte[] data = page.getPageData();
        raf.write(data);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            return (int) Math.ceil(raf.length() / BufferPool.getPageSize());
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read");
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modified = new ArrayList<>();
        boolean inserted = false;
        for (int i = 0 ; i < this.numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            try {
                page.insertTuple(t);
                page.markDirty(true, tid);
                modified.add(page);
                inserted = true;
                break;
            } catch (DbException e) {

            }
        }
        if (!inserted) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(newPage);
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            modified.add(newPage);
        }
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        // finding the corresoponding page
        if (pageId.pageNumber() < this.numPages()) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            page.deleteTuple(t);
            modifiedPages.add(page);
        }
        if (modifiedPages == null)
            throw new DbException("target tuple does not exist");
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, Permissions.READ_ONLY);
    }

    //创建private类型
    //heapfile基本结构：包含多个page
    private class HeapFileIterator implements DbFileIterator{
        //basic fields
        public TransactionId tid;
        public Permissions perm;
        public int currPos;//当前页面位置
        public HeapPage currPage;
        public Iterator<Tuple> it;//当前页面iterator
        public boolean open;

        //constructor
        public HeapFileIterator(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
            currPos = 0;
            open = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            //基本逻辑
            //获取当前页面(BufferPool.getPage())
            //获取当前页面的iterator
            currPage = (HeapPage) Database.getBufferPool().
                    getPage(tid, new HeapPageId(getId(), currPos), perm);
            it = currPage.iterator();
            open = true;

        }

        @Override
        //基本逻辑
        //查看当前it是否有下一个：有则返回
        //如果没有：在numPages内探测下一页
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open) return false;
            if (it.hasNext()) return true;
            while (currPos < numPages()-1) {
                currPos++;
                open();
                if (it.hasNext()) return true;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            open = false;
            this.currPos = 0;
            this.currPage = null;
            this.it = null;
        }

        //hasNext()

        //next()
    }
}

