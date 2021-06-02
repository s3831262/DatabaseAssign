package com.dbassignment;
import java.io.*;

public class HeapBTree {

    public static String PAGE_PATH = "disc/page";

    public static Record findByID(int recordID) {
        int size = HeapPage.size / Record.bytes();
        int pageNum = (recordID) / size;
        int recordNum = (recordID) % size;
        HeapPage page = HeapPage.readFromFile(PAGE_PATH + "/" + pageNum + ".page");
        return page.getRecords()[recordNum];
    }

    public static void main(String[] args) {
        HeapFile hf = new HeapFile();
        hf.generateHeapFile("D:\\znotebook\\DBproject\\src\\com\\dbproject\\A1_derbyData\\derby-sensors-recordings.csv");
    }

    public void generateHeapFile(String path) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            reader.readLine();
            String line;
            long count = 0;
            int size = HeapPage.size / Record.bytes();
            HeapPage page = null;
            while((line = reader.readLine())!= null){
                int index = (int) count % size;
                if (index == 0) {
                    if (count != 0) {
                        page.writeToFile(PAGE_PATH, size);
                        System.out.println("write page No." + (count / size - 1));
                    }
                    page = new HeapPage((int)(count / size));
                }
//                System.out.println(line);
                String item[] = line.split(",");
                page.addRecord(index, item);
                count++;
            }
            int remainder = (int)((count-1) % size);
            if (remainder != 0) {
                page.writeToFile(PAGE_PATH, remainder);
                System.out.println("write page No." + count / size);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
class HeapPage {
    private int pageID;
    public static final int size = 8192; // bytes
    private Record[] records;

    public HeapPage(int pageID) {
        this.pageID = pageID;
        records = new Record[size/Record.bytes()];
    }

    public Record[] getRecords() {
        return records;
    }

    public int getPageID() {
        return pageID;
    }

    public void writeToFile(String dir, int len) {
        File file = new File(dir + "/" + pageID + ".page");
        try {
            createFile(file, size);
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            for (int i = 0; i < len; i++) {
                out.write(records[i].toByteStream());
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean addRecord(int index, String[] item) {
        if (index >= records.length || item.length != 10)
            return false;
        records[index] = new Record(Integer.parseInt(item[0]), item[1], Integer.parseInt(item[2]),item[3],
                Integer.parseInt(item[4]), item[5], Integer.parseInt(item[6]), Integer.parseInt(item[7]),
                item[8], Integer.parseInt(item[9]));
        return true;
    }


    public static byte[] readBytesFromInputStream(InputStream in,
                                                  int len) throws IOException {
        int readSize;
        byte[] bytes = new byte[len];
        long length_tmp = len;
        long index = 0;// start from zero
        while ((readSize = in.read(bytes, (int) index, (int) length_tmp)) != -1) {
            length_tmp -= readSize;
            if (length_tmp == 0) {
                break;
            }
            index = index + readSize;
        }
        return bytes;
    }

    public static HeapPage readFromFile(String path) {
        String[] pathArr = path.split("/");
        String fileName = pathArr[pathArr.length - 1];
        int pageID = Integer.parseInt(fileName.substring(0, fileName.length() - 5));
        HeapPage page = new HeapPage(pageID);

        File file = new File(path);
        InputStream in = null;
        try {
            System.out.println("read page file: " + path);
            in = new FileInputStream(file);
            for (int i = 0; i < page.records.length; i++) {

                byte[] tempbytes = readBytesFromInputStream(in, Record.bytes());
                page.records[i] = Record.byteToRecord(tempbytes, 0);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return page;
    }

    public static void createFile(File file, long length) throws IOException{
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            r.setLength(length);
        } finally{
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
//        System.out.println(Record.bytes());
        HeapPage page = new HeapPage(1);
//
        HeapPage newPage = HeapPage.readFromFile("disc/page/33.page");
        for (int i = 0; i < page.records.length; i++) {
            System.out.println(newPage.records[i]);
        }
    }

}
