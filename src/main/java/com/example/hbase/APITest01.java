package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase API DDL
 *  1.判断表是否存在
 *  2.创建表
 *  3.创建命名空间
 *  4.删除表
 * HBase API DML
 *  5.插入数据
 *  6.查数据（get）
 *  7.查数据（scan）
 *  8.删除数据
 */
public class APITest01 {
    private static Connection connection;
    private static Admin admin;

    public static void main(String[] args) throws IOException, DeserializationException {
//        System.out.println(isTableExistOld("wxl_test_1"));
        init();

        // 判断表是否存在
//        System.out.println(isTableExist("wxl_test_1"));

        // 创建表
//        createTable("wxl_test_2","cf_w1","cf_w2");
//        System.out.println(isTableExist("wxl_test_2"));

        // 新增列族
        addFamily("wxl:test_1", "cf1");

        // 删除表
//        dropTable("wxl_test_1");

        // 创建命名空间
//        createNameSpace("wxl");

        // 在指定命名空间下创建表
//        createTable("wxl:test_1", "cf1", "cf2");
//        System.out.println(isTableExist("wxl:test_1"));

        // 添加数据
//        putData("wxl:test_1", "1002", "cf2", "name", "C33");

        // 获取数据
//        getData("wxl:test_1", "1002", "cf2", "name");

        // 扫描表
//        scanTable("wxl:test_1");


        close();
    }

    // 8.删除数据（addColumn：此操作慎用，建议使用addColumns操作）
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除指定列的最新版本（次操作慎用，特别是在版本数为1的时候）
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 删除指定列的指定版本
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn),1583838527374L);
        // 删除指定列的所有版本
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 删除时间戳小于或等于指定时间戳的所有指定列的版本
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn),1583838527374L);

        // 删除数据
        table.delete(delete);

        // 关闭资源
        table.close();
    }

    // 7.获取数据（scan）
    public static void scanTable(String tableName) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 构建扫描对象
        Scan scan = new Scan(Bytes.toBytes("1001") ,Bytes.toBytes("1001"));
        // 设置最大版本数
        scan.setMaxVersions(5);

        // 扫描表获取数据
        ResultScanner scanner = table.getScanner(scan);

        // 解析数据
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.print("rowKey:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print("/cf:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("/cn:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.print("/value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }

        // 关闭资源
        table.close();

    }

    // 6.获取数据（get）
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 构建查询条件
        Get get = new Get(Bytes.toBytes(rowKey));
        // 设置列族
        get.addFamily(Bytes.toBytes(cf));
        // 设置列名
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 设置最大版本数
        get.setMaxVersions(5);


        // 获取数据
        Result result = table.get(get);

        // 遍历数据
        for (Cell cell : result.rawCells()) {
            System.out.print("cf: " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.print("/cn: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.print("/value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println();
        }

        // 关闭资源
        table.close();
    }

    // 5.添加数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 构建数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        // 添加数据
        table.put(put);

        // 关闭资源
        table.close();
    }

    // 4.创建命名空间
    public static void createNameSpace(String nameSpace) {
        // 获取命名空间描述器
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();

        try {
            // 创建命名空间
            admin.createNamespace(descriptor);
        }catch (NamespaceExistException e1){
            System.out.println(nameSpace + " 命名空间已存在");
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 3.删除表
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println(tableName + " 表不存在");
            return;
        }

        // 禁用表
        admin.disableTable(TableName.valueOf(tableName));

        // 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    // 2.1修改表，添加列族
    public static void addFamily(String tableNameStr, String familyStr) throws IOException {
        // 创建需要操作的表对象
        TableName tableName = TableName.valueOf(tableNameStr);

        // 创建列族描述器
        HColumnDescriptor family = new HColumnDescriptor(familyStr);

        // 添加列族
        admin.addColumn(tableName, family);
    }

    // 2.创建表
    public static void createTable(String tableName, String ...ags) throws IOException, DeserializationException {
        // 判断是否存在列族信息
        if (ags.length <= 0){
            System.out.println("请添加至少1个列族");
            return;
        }

        // 判断表是否存在
        if (isTableExist(tableName)){
            System.out.println(tableName + " 表已存在");
            return;
        }

        // 创建表描述器
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 添加列族信息
        for (String ag : ags) {
            // 创建列族描述器
            HColumnDescriptor family = new HColumnDescriptor(ag);

            descriptor.addFamily(family);
        }


        // 创建表
        admin.createTable(descriptor);

    }


    // 1.判断表是否存在(新API)
    public static boolean isTableExist(String tableName) throws IOException {

        return admin.tableExists(TableName.valueOf(tableName));
    }

    // 1.判断表是否存在(旧API)
    public static boolean isTableExistOld(String tableName) throws IOException {
        // 配置文件信息
        HBaseConfiguration conf = new HBaseConfiguration();
        // 配置zookeeper信息
        conf.set("hbase.zookeeper.quorum", "slave4,slave6,slave5");

        // 获取管理员对象
        HBaseAdmin admin = new HBaseAdmin(conf);

        // 判断表是否存在
        boolean exists = admin.tableExists(tableName);

        // 关闭资源
        admin.close();
        return exists;
    }

    // 初始化环境
    public static void init() throws IOException {
        // 配置文件信息
        Configuration conf = HBaseConfiguration.create();
        // 配置zookeeper信息
        conf.set("hbase.zookeeper.quorum", "slave4,slave6,slave5");

        // 获取管理员对象
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    // 关闭资源
    public static void close() throws IOException {
        if (admin != null){
            admin.close();
        }
        if (connection != null){
            connection.close();
        }
    }
}
