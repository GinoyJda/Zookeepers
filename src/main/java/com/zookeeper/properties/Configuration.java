package com.zookeeper.properties;

/**
 * Created by yanjd on 2017/12/29.
 */

import java.io.*;
import java.util.Properties;

/**
 * 读取properties配置文件
 */
public class Configuration {
    private Properties pro;
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    private String filepath;

    public Configuration() {

        //重要内容


        //测试地址
        filepath="\\test.properties";

        pro = new Properties();
        try {
            fileInputStream = new FileInputStream(filepath);
            pro.load(fileInputStream);
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    /**
     * 根据key取得键值
     */
    public String getValue(String key) {
        if (pro.containsKey(key)) {
            String value = pro.getProperty(key);
            return value;
        } else {
            return "";
        }
    }


    /**
     * 改变或者添加一个key的值
     * 当key存在于properties文件中时，修改key的值， 当key不存在时，添加键值对
     * @param key:要存入的键
     * @param value:要存入的值
     */
    public void setValue(String key, String value) {
        pro.setProperty(key, value);
    }

    /**
     * 将更改后的文件数据存入指定的文件中
     */
    public void saveFile(String fileName) {
        try {
            fileOutputStream = new FileOutputStream(fileName);
            pro.store(fileOutputStream, "");
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void renameFile(String path,String oldname,String newname){
        if(!oldname.equals(newname)){//新的文件名和以前文件名不同时,才有必要进行重命名
            File oldfile=new File(path+"/"+oldname);
            File newfile=new File(path+"/"+newname);
            if(!oldfile.exists()){
                return;//重命名文件不存在
            }
            if(newfile.exists())//若在该目录下已经有一个文件和新文件名相同，则不允许重命名
                System.out.println(newname+"已经存在！");
            else{
                oldfile.renameTo(newfile);
            }
        }else{
            System.out.println("新文件名和旧文件名相同...");
        }
    }
    /**
     * 测试方法
     */
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setValue("server.addr","10.176.63.14");
        conf.renameFile("E:\\","test.properties","test_bak.properties");
        String filename="\\test1.properties";
        conf.saveFile(filename);


    }
}