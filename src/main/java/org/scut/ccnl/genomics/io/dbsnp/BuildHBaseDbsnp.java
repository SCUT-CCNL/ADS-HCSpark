package org.scut.ccnl.genomics.io.dbsnp;


import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.GlobalArgument;
import org.scut.ccnl.genomics.io.HbaseDAO;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.scut.ccnl.genomics.io.fasta.DistributedFastaSequenceFile.getSequenceDictionary;

public class BuildHBaseDbsnp {
    Configuration conf = HBaseConfiguration.create();
    Connection connect;
    TableName tn;
    Admin admin;
    Table table;

    final String[] cols = {"base","other"};

    GenomeLocParser parser;

    public BuildHBaseDbsnp() throws IOException {
        conf.addResource("hbase-site.xml");
        conf.set("hbase.htable.threads.max","128");

        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        connect = ConnectionFactory.createConnection(conf);
        tn = TableName.valueOf("dbsnp");
        admin = connect.getAdmin();

        parser = new GenomeLocParser(getSequenceDictionary(Arguments.FASTA_DICT));
        parser.setMRUCachingSAMSequenceDictionary();
    }

    public void getDbsnpTable() throws IOException {
        if(admin.tableExists(tn)){
            System.out.println("table is exists!");

        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            for(String col : cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        table = connect.getTable(tn);

    }

    public Put insert(String[] sublines){
        String chrom = sublines[0];
//        int pos = Integer.valueOf(sublines[1]);

        Put put = new Put(Bytes.toBytes(HbaseDAO.getDbsnpRowKey(parser,chrom,sublines[1])));
        put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes("chrom"),Bytes.toBytes(chrom));
        put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes("pos"),Bytes.toBytes(sublines[1]));
        put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes("rsid"),Bytes.toBytes(sublines[2]));
        put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes("ref"),Bytes.toBytes(sublines[3]));
        put.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes("alt"),Bytes.toBytes(sublines[4]));
        put.addColumn(Bytes.toBytes(cols[1]),Bytes.toBytes("qual"),Bytes.toBytes(sublines[5]));
        put.addColumn(Bytes.toBytes(cols[1]),Bytes.toBytes("filter"),Bytes.toBytes(sublines[6]));
        put.addColumn(Bytes.toBytes(cols[1]),Bytes.toBytes("info"),Bytes.toBytes(sublines[7]));

        return put;
    }

    public void commit(List<Put> puts) {
        try {
            table.put(puts);
        } catch (Exception e) {
            System.out.println("myerror:"+puts.get(0).getRow());
            System.out.println("myerror:"+puts.get(puts.size()-1).getRow());

            e.printStackTrace();
        }
    }

    public void commit(Put put) throws IOException {
        table.put(put);
    }



    public static void buildDbsnp(CommandLine commandLine) throws IOException {


        BuildHBaseDbsnp buildHBaseDbsnp = new BuildHBaseDbsnp();

        try(BufferedReader reader= new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(commandLine.getOptionValue("i")))));
            ) {

            int num = 1;
            String line =null;
            List<Put> puts = new ArrayList<>();

            buildHBaseDbsnp.getDbsnpTable();


            while((line=reader.readLine())!=null){

                if(line.startsWith("#")) continue;

                String[] sublines = line.split("\t");
                puts.add(buildHBaseDbsnp.insert(sublines));

                if(num++%100000==0){//100000
                    buildHBaseDbsnp.commit(puts);
                    puts.clear();
                }

            }
            buildHBaseDbsnp.commit(puts);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
