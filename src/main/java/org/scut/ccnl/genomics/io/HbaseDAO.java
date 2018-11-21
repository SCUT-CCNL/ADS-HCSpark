package org.scut.ccnl.genomics.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.scut.ccnl.genomics.Arguments;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static org.scut.ccnl.genomics.io.fasta.DistributedFastaSequenceFile.getSequenceDictionary;


public class HbaseDAO implements Closeable {


    static class HbaseConnection{
        static HBaseClient client;
        static {
            Configuration conf = HBaseConfiguration.create();
            client = new HBaseClient("cu09,cu11,cu10");
        }

        public static Scanner getScanner(){
            return client.newScanner("dbsnp");
        }

    }


    public static Map<String,String> contigHash = new ConcurrentHashMap<>();
    public static Map<String,String> dbsnpContigHash = new HashMap<>();

    public HbaseDAO(){

    }



    public static String getDbsnpRowKey(GenomeLocParser parser, String contig, String id){
        String contighash = String.format("%03d",parser.getContigIndex(contig));
        //可能要注意这个的效率问题
        return contighash + String.format("%09d",Integer.valueOf(id));
    }

    public static String getDbsnpRowKey(GenomeLocParser parser, String contig, int id){
        String contighash = String.format("%03d",parser.getContigIndex(contig));
        //可能要注意这个的效率问题
        return contighash + String.format("%09d",id);
    }


    public static String getFastaRowkey(String contig, long id){
        String contighash;
        if(!dbsnpContigHash.containsKey(contig))
            dbsnpContigHash.put(contig,String.format("%010d",contig.hashCode()& 0x7FFFFFFF));
        contighash = contigHash.get(contig);

        //可能要注意这个的效率问题
        return contighash+ String.format("%06d",id);
    }



    public static void main(String[] args) {
        try {
            GenomeLocParser parser = new GenomeLocParser(getSequenceDictionary(Arguments.FASTA_DICT));
//            new HbaseDAO().buildDbsnp(parser);
//            new HbaseDAO().buildReferenceDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /* range scan table. */
    public ResultScanner getResultScan(String tableName, String start_rowkey,
                                     String stop_rowkey,String family,String column) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

        return null;
//        return HbaseConnection.getTable().getScanner(scan);
    }

    public static List<ArrayList<KeyValue>> getResultScan(String start_rowkey,
                                                          String stop_rowkey, String family) throws IOException {

        Scanner scanner = HbaseConnection.getScanner();
        scanner.setFamily(family.getBytes());
        scanner.setStartKey(start_rowkey);
        scanner.setStopKey(stop_rowkey);
        try {
            List<ArrayList<KeyValue>> keyValues = scanner.nextRows().join(30000);
            if(keyValues!=null)
                return keyValues;
            else
                return Collections.emptyList();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            scanner.close();
        }
        return Collections.emptyList();
    }




    @Override
    public void close() throws IOException {
    }

}
