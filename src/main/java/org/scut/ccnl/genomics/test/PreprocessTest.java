package org.scut.ccnl.genomics.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;


public class PreprocessTest {
    public static void main(String[] args) throws Exception {
        //读取 128MB分块的文件
        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\Master\\实验\\err572-time.csv"));
//        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\Master\\实验\\BGI-time.csv"));
        //第一行去掉
        String line = bufferedReader.readLine();
        List<Entity> timeList = new ArrayList<>();
        while((line = bufferedReader.readLine())!=null){
            String[] splits = line.split(",");
            timeList.add(new Entity(splits[0],splits[1],splits[2],splits[6],splits[9]));
        }
        bufferedReader.close();
        //读取 16MB分块的文件
        bufferedReader = new BufferedReader(new FileReader("E:\\Master\\实验\\ERR091572.sorted.rg.md.bqsr.bam.hcidx.16"));
//        bufferedReader = new BufferedReader(new FileReader("E:\\Master\\实验\\NA12878.16MB.hcidx"));
        //第一行去掉
        line = bufferedReader.readLine();
        List<HcidxEntity> hcidxList = new ArrayList<>();
        List<HcidxSubEntity> subEntities = new ArrayList<>(8);
        while((line = bufferedReader.readLine())!=null){
            if(subEntities.size()==8){
                hcidxList.add(new HcidxEntity(subEntities));
                subEntities.clear();
            }
            String[] splits = line.split(":");
            subEntities.add(new HcidxSubEntity(splits[1],splits[5],splits[6]));
        }
        hcidxList.add(new HcidxEntity(subEntities));



        int timeListSize = timeList.size();
        int tenPercent = (int) Math.ceil(timeListSize*0.1);
        int twentySixPercent = (int) Math.ceil(timeListSize*0.26);
        int thirteenPercent = (int) Math.ceil(timeListSize*0.13);
        int twentyPercent = (int) Math.ceil(timeListSize*0.2);
        int fivePercent = (int) Math.ceil(timeListSize*0.05);

        timeList.sort(Comparator.comparing(Entity::getTime));
        //预测time 10 %
        TreeSet<Integer> real = new TreeSet<Integer>();
        for (int i = 0; i < tenPercent; i++) {
            real.add(timeList.get(timeListSize-1-i).id);
        }

        //预测time 5 %
        TreeSet<Integer> realFive = new TreeSet<Integer>();
        for (int i = 0; i < fivePercent; i++) {
            realFive.add(timeList.get(timeListSize-1-i).id);
        }


        TreeSet<Integer> predict = new TreeSet<Integer>();
        //（cigarI+cigarD）为前17%，后5%
        timeList.sort(Comparator.comparing(Entity::getI_d));
        for (int i = 0; i < twentySixPercent; i++) {
            predict.add(timeList.get(timeListSize-1-i).id);
        }
        for (int i = 0; i < fivePercent; i++) {
            predict.add(timeList.get(i).id);
        }
        //Record后20%
        timeList.sort(Comparator.comparing(Entity::getRecord));
        for (int i = 0; i < twentyPercent; i++) {
            predict.add(timeList.get(i).id);
        }

        //interval的前13%、后5%
        timeList.sort(Comparator.comparing(Entity::getInterval));
        for (int i = 0; i < fivePercent; i++) {
            predict.add(timeList.get(i).id);
        }
        for (int i = 0; i < thirteenPercent; i++) {
            predict.add(timeList.get(timeListSize-1-i).id);
        }

//        List<HcidxEntity> filter = hcidxList.stream()
//                .filter(hcidxEntity -> hcidxEntity.type>0 && hcidxEntity.type<=3 && predict.contains(hcidxEntity.getOriginalID()))
//                .collect(Collectors.toList());
        Set<Integer> filter = hcidxList.stream()
                .filter(hcidxEntity -> hcidxEntity.type>0 && hcidxEntity.type<=3
                        && predict.contains(hcidxEntity.getOriginalID()))
                .map(hcidxEntity -> hcidxEntity.getOriginalID())
                .collect(Collectors.toSet());
//
//        real.removeAll(filter);
//        filter.sort(Comparator.comparing(HcidxEntity::getOriginalID));

        realFive.removeAll(filter);

        System.out.println();


    }
}


class Entity{
    public int id;
    public int interval;
    public int record;
    public int i_d;
    public int time;

    public Entity(int id, int interval, int record, int i_d, int time) {
        this.id = id;
        this.interval = interval;
        this.record = record;
        this.i_d = i_d;
        this.time = time;
    }

    public Entity(String id, String interval, String record, String i_d, String time) {
        this.id = Integer.valueOf(id);
        this.interval = Integer.valueOf(interval);
        this.record = Integer.valueOf(record);
        this.i_d = Integer.valueOf(i_d);
        this.time = Integer.valueOf(time);
    }

    public int getId() {
        return id;
    }

    public int getInterval() {
        return interval;
    }

    public int getRecord() {
        return record;
    }

    public int getI_d() {
        return i_d;
    }

    public int getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "id=" + id +
                ", interval=" + interval +
                ", record=" + record +
                ", i_d=" + i_d +
                ", time=" + time +
                '}';
    }
}


class HcidxEntity{
    public int id=-1;
    public int originalID=-1;

    public double maxVsAll;
    public double maxVsLarge2;
    public int type=-1;
    public List<HcidxSubEntity> entities;

    public HcidxEntity(List<HcidxSubEntity> subEntities){
        id = subEntities.get(0).id;

        originalID = id /8;

        if(originalID>=540){
            int a = 9;
        }

        int all=subEntities.stream().map(HcidxSubEntity::getI_d).reduce(0,Integer::sum);
        subEntities.sort(Comparator.comparing(HcidxSubEntity::getI_d).reversed());
        maxVsAll = ((double) subEntities.get(0).getI_d())/all;
        if(subEntities.size()>1)
            maxVsLarge2 = ((double) subEntities.get(0).getI_d())/subEntities.get(1).getI_d();
        else
            maxVsLarge2 = 1;

        if(maxVsAll > 0.3){
            if(maxVsLarge2>1.5)
                type = 1;
            else
                type = 2;
        }else if(maxVsAll>0.24){
            if(maxVsLarge2<1.7)
                type = 3;
        }else if(maxVsAll>0.19){
            if(maxVsLarge2<1.5){
                type = 4;
            }
        }else if(maxVsAll<0.18){
            type = 5;
        }
        entities = new ArrayList<>(subEntities);

    }

    public int getId() {
        return id;
    }

    public int getOriginalID() {
        return originalID;
    }

    public double getMaxVsAll() {
        return maxVsAll;
    }

    public double getMaxVsLarge2() {
        return maxVsLarge2;
    }

    public int getType() {
        return type;
    }

    @Override
    public String toString() {
        return "HcidxEntity{" +
                "id=" + id +
                ", originalID=" + originalID +
                ", maxVsAll=" + maxVsAll +
                ", maxVsLarge2=" + maxVsLarge2 +
                ", type=" + type +
                '}';
    }
}

class HcidxSubEntity{
    public int id;
    public int i_d;

    public HcidxSubEntity(String id, String i_d) {
        this.id = Integer.valueOf(id);
        this.i_d = Integer.valueOf(i_d);
    }

    public HcidxSubEntity(String id, String i,String d) {
        this.id = Integer.valueOf(id);
        this.i_d = Integer.valueOf(i)+Integer.valueOf(d);
    }

    public int getId() {
        return id;
    }

    public int getI_d() {
        return i_d;
    }

    @Override
    public String toString() {
        return "HcidxSubEntity{" +
                "id=" + id +
                ", i_d=" + i_d +
                '}';
    }
}