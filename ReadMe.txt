├─main
│  ├─java
│  │  └─sync
│  │      ├─conf   --read properties file
│  │      ├─main   --main
│  │      ├─mapper --mapper
│  │      └─reduce --reduce
│  └─resources	   --.properties file
└─test			   --test code
    └─java
        └─sync
1、在配置文件里面配置机器的hadoop主机ip和端口
2、在config.properties文件里面配置headers，列名，用"|"分开
3、将整个maven工程打成jar包，上传到hadoop主机中
4、用命令
HADOOP_CLASSPATH=${HBASE_HOME}/lib/hbase-protocol-0.98.1-cdh5.1.0.jar:/etc/hbase/conf hadoop jar path_filename package_class_name -tb tableName
参数解释：
package_class_name ： jar包的路径和包名  如：/usr/local/cdh/hbaseSync-0.0.1-SNAPSHOT-jar-with-dependencies.jar
tableName : 目标表名字，即将数据存入到哪个表里面。