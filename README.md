## 修改需求
改为多线程，获取指定目录下的所有Data文件，分别解析后保存到HDFS目录下，数据格式CSV，不需要列名
1、先修改数据格式解析代码，输出csv格式
2、增加hdfs文件写入代码
3、改为指定sstable路径和hdfs目录
3、增加多线程调度

## 构建代码
```
mvn clean package -DskipTests
```
## 运行
```
java -jar \
-Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib \
-Dorg.xerial.snappy.tempdir=/tmp    \
target/SSTableExport-1.0-SNAPSHOT.jar  \
/var/lib/cassandra/data/keyspace1/users/ \
hdfs://192.168.3.1:8020/test/
```
