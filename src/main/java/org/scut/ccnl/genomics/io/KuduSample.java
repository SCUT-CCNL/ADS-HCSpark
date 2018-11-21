package org.scut.ccnl.genomics.io;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class KuduSample {

    private static final String KUDU_MASTER = System.getProperty(
            "kuducu10", "192.168.0.10:7051");
    private static final String path = "/data/home/wzz/workstation/dbsnp/dbsnp_135.hg19.modify.vcf";
    public static final String openFileStyle = "r";
    public static final String fieldLimitChar = "\t";

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("-----------------------------------------------");
        System.out.println("input the tablename please!");
//        Scanner in = new Scanner(System.in);
        String tableName = "dbsnp";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            List<ColumnSchema> columns = new ArrayList<>(8);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("chrom", Type.STRING)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("pos", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("rs_id", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("ref", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("alt", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("qual", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("filter", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("info", Type.STRING)
                    .build());

            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("pos");
            if(!client.tableExists(tableName))
                createTable(client,columns,tableName,rangeKeys);

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            //读取文件&插入记录
            RandomAccessFile raf = new RandomAccessFile(path, openFileStyle);
            String line_record = raf.readLine();
            while (line_record != null) {
                if(line_record.startsWith("#")) continue;

                String[] fields = line_record.split(fieldLimitChar);
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString(0, fields[0]);//chrom
                row.addInt(1, Integer.parseInt(fields[1]));//pos
                row.addString(2, fields[2]);//rs_id
                row.addString(3, fields[3]);//ref
                row.addString(4, fields[4]);//alt
                row.addString(5, fields[5]);//qual
                row.addString(6, fields[6]);//filter
                row.addString(7, fields[7]);//info

                session.apply(insert);

                line_record = raf.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void createTable(KuduClient client, List<ColumnSchema> columns,
                                   String tableName, List<String> rangeKeys) throws Exception{
        Schema schema = new Schema(columns);
        client.createTable(tableName, schema,
                new CreateTableOptions().setRangePartitionColumns(rangeKeys));
    }

}
