

------------------------------------------------------------------------------------------
                                      [ 环境变量 ]
------------------------------------------------------------------------------------------
export ZK_HOME=/root/zookeeper/zookeeper-3.4.10
export HBASE_HOME=/root/hbase/hbase-1.2.6
export MYSQL_HOME=/root/mysql/mysql-5.6.33-linux-glibc2.5-x86_64
export HADOOP_HOME=/root/data/hadoop-2.7.5
export SCALA_HOME=/root/scala/scala-2.11.8
export HIVE_HOME=/root/hive/apache-hive-2.3.3-bin
export JAVA_HOME=/usr/local/jdk1.8.0_161
export CLASSPATH=$JAVA_HOME/lib:$JAVA_HOME/jre/lib
export SPARK_HOME=/root/spark/spark-2.3.0-bin-hadoop2.7
export SQOOP_HOME=/root/sqoop/sqoop-1.4.7.bin__hadoop-2.6.0
export PATH=$PATH:$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SCALA_HOME/b
in:$MYSQL_HOME/bin:$HBASE_HOME/bin:$ZK_HOME/bin:$SQOOP_HOME/bin



------------------------------------------------------------------------------------------
                                      [ 安装hdfs ]
------------------------------------------------------------------------------------------
    解压文件:   tar -zxvf hadoop-2.7.5.tar.gz
	
	1 修改 core-site.xml
	进入路径 /root/data/hadoop-2.7.5/etc/hadoop
	表示hdfs基本路径,默认端口9000
		<configuration>
		   <property>
				<name>fs.defaultFS</name>
				<value>hdfs://master:9000</value>
		   </property>
		</configuration>
		
		
	2 修改 hdfs-site.xml
	dfs.replication：数据库备份数量,不能大于DataNode数量
	dfs.datanode.data.dir：datanode存储数据地方
	dfs.namenode.data.dir：namenode存储数据地方
		 <configuration>
		   <property>
			   <name>dfs.replication</name>
			   <value>1</value>
		   </property>
		   <property>
				<name>dfs.datanode.data.dir</name>
				<value>/root/data/hdfs/name</value>
		   </property>
		   <property>
				<name>dfs.namenode.data.dir</name>
				<value>/root/data/hdfs/data</value>
		   </property>
		</configuration>

		
		
	3 创建对应的存储目录
	创建datanode和namenode 对应的文件夹目录
      mkdir /root/data/hdfs/name
      mkdir /root/data/hdfs/data
		
	4 修改 hadoop-env.sh
	export JAVA_HOME=/usr/local/jdk1.8.0_161
	
	5 创建 slaves
	hdfs是一个分布式存储的,需要让master知道对应的slave
	在 /root/hdfsV2.0/hadoop-2.7.5/etc/hadoop 路径下新建slaves文件
	添加 node-1
	     node-2
    
	6 配置环境变量
	vim  /etc/profile
	source /etc/profile
	
	
	7.master节点配置好的hadoop分发到slave节点
	scp -r /root/data root@node-2:/root/
	
	
	8.运行hafs
	先对格式化: hdfs  namenode  -formats
	启动start-dfs.sh
    启动stop-dfs.sh
     
    输入HDFS默认监控地址：http://master:50070

------------------------------------------------------------------------------------------
                                      [ 安装zookeeper ]
------------------------------------------------------------------------------------------

zookeepe 分布式协调的管理者

	1.修改zoo.cfg文件
	/root/zookeeper/zookeeper-3.4.10/conf
	设置文件目录和日志目录; server.0/1/2表示分别在三台机器上分别安装zookeeper服务
	tickTime=2000
	initLimit=10
	syncLimit=5
	dataDir=/root/zookeeper/data
	dataLogDir=/root/zookeeper/log
	clientPort=2181
	#server.1=192.168.5.134:2888:3888
	server.0=master:8880:7770
	server.1=node-1:8881:7771
	server.2=node-2:8882:7772

	2.master节点配置好的文件分发到slave节点
	scp -r /root/zookeeper root@node-1:/root/
    scp -r /root/zookeeper root@node-2:/root/

	3.在三台机器上新增myid文件
	进入/root/zookeeper/data 路径,分别创建文件输入0 1 2 . 存放的标识本台服务器的文件
	是整个zk集群用来发现彼此的一个重要标识
	在master机器上 vim myid  输入 0
	在node-1机器上 vim myid  输入 1
	在node-2机器上 vim myid  输入 2


    4.配置环境变量
	在三台机器上都配置对应的环境变量
	vim  /etc/profile
	source  /etc/profile
	
	5.分别到三台机器上开启或关闭zookeeper 
	启动:  zkServer.sh start 
	关闭:  zkServer.sh stop
	查看状态:   zkServer.sh status

	

------------------------------------------------------------------------------------------
                                      [ 安装Spark ]
------------------------------------------------------------------------------------------

	1.解压文件
	tar -zxvf spark-2.3.0-bin-hadoop2.7.tgz


	2.配置slaves
	进入路径 /root/spark/spark-2.3.0-bin-hadoop2.7/conf
	复制文件 cp slaves.template slaves
	修改文件 vim slaves,  输入 node-1 node-2


	3.配置spark-env.sh
	复制文件  cp spark-env.sh.template  spark-env.sh
	修改文件  vim spark-env.sh , 输入$JAVA_HOME
	  export JAVA_HOME=/usr/local/jdk1.8.0_161


	4.master节点配置好的文件分发到slave节点
		scp -r /root/spark root@node-1:/root/
		scp -r /root/spark root@node-2:/root/


	5.配置环境变量
	  Spark安装路径 /root/spark/spark-2.3.0-bin-hadoop2.7
	  vim  /etc/profile
	  source  /etc/profile


	 6.web端打开spark监控窗口
	 http://master:8080/
	 

------------------------------------------------------------------------------------------
                                      [ 安装Hive ]
------------------------------------------------------------------------------------------


      1.解压hive安装包
	  tar -zxvf apache-hive-2.3.3-bin.tar.gz 


	  2.创建配置文件hive-site.xml
	  在 /root/hive/apache-hive-2.3.3-bin/conf 路径下新建配置文件hive-site.xml
	  
 <configuration>
  	<property>
           <name>javax.jdo.option.ConnectionURL</name>
           <value>jdbc:mysql://master:3306/hive?createDatabaseIfNotExist=true</value>
           <description>JDBC connect string for a JDBC metastore</description>    
	</property>   
	<property> 
           <name>javax.jdo.option.ConnectionDriverName</name> 
           <value>com.mysql.jdbc.Driver</value> 
           <description>Driver class name for a JDBC metastore</description>     
	</property>               
 
	<property> 
            <name>javax.jdo.option.ConnectionUserName</name>
            <value>root</value>	
            <description>username to use against metastore database</description>
	</property>
	<property>  
            <name>javax.jdo.option.ConnectionPassword</name>
            <value>root</value>
            <description>password to use against metastore database</description>  
	</property>    

        <property>  
             <name>hive.metastore.warehouse.dir</name>  
             <value>/root/hive/warehouse</value>  
        </property>
        <property>
             <name>hbase.zookeeper.quorum</name>
             <value>master,node-1,node-2</value>
        </property>
  </configuration>


	3.上传驱动包
	/root/hive/apache-hive-2.3.3-bin/lib
	因为通过JDBC的方式访问MySQL,所以将驱动包mysql-connector-java-5.1.44-bin.jar 传到lib下面



	4.MySQL作为元数据管理,初始化配置
	$HIVE_HOME/bin/schematool  -dbType mysql -initSchema


------------------------------------------------------------------------------------------
                                      [ 安装Hbase ]
------------------------------------------------------------------------------------------

	1.解压hbase安装包
	tar -zxvf hbase-1.2.6-bin.tar.gz

	2.修改 hbase-site.xml 配置
	进入 /root/hbase/hbase-1.2.6/conf 路径 
	hbase.rootdir 是hbase数据在hdfs上存储路径
	hbase.cluster.distributed 分布式存储
	hbase.zookeeper.quorum 使用zookeeper管理

	<configuration>
	  <property>
		<name>hbase.rootdir</name>
		<value>hdfs://master:9000/hbase</value>
	  </property>
	  <property>
		<name>hbase.cluster.distributed</name>
		<value>true</value>
	  </property>
	  <property>
		<name>hbase.zookeeper.quorum</name>
		<value>master,node-1,node-2</value>
	  </property>
	</configuration>


	3.修改 hbase-env.sh 配置
	进入 /root/hbase/hbase-1.2.6/conf 路径
	export JAVA_HOME=/usr/local/jdk1.8.0_161
	export HBASE_CLASSPATH=/root/data/hadoop-2.7.5/etc/hadoop
	export HBASE_MANAGES_ZK=false


	4.修改regionservers 配置
	进入 /root/hbase/hbase-1.2.6/conf 路径
	vim regionservers
	node-1
	node-2


	5.将master配置拷贝到slave
	scp -r /root/hbase root@node-1:/root/
	scp -r /root/hbase root@node-2:/root/


	6.配置hbase环境变量
	vim /etc/profile
	source /etc/profile

	7.启动hbase
	先需要启动hdfs和zookeeper
	start-dfs.sh
	zkServer.sh start
	start-hbase.sh
	访问hbase的web监控  http://master:16010

    hbase shell


------------------------------------------------------------------------------------------
                                      [ 安装kafka ]
------------------------------------------------------------------------------------------
 
	https://www.apache.org/dyn/closer.cgi?path=/kafka/1.0.0/
    
	1.解压hbase安装包
	tar -zxvf kafka_2.11-1.0.0.tgz
	
	
	2.修改配置
    cd /root/kafka/kafka_2.11-1.0.0/config
	vim server.properties
	broker.id=0
	log.dirs=/root/kafka/kafkalogs/
	zookeeper.connect=192.168.5.134:2181

	对应创建log日志文件夹

	3.将master配置拷贝到slave
		scp -r /root/kafka root@node-1:/root/
		scp -r /root/kafka root@node-2:/root/


	4.在slave节点配置
	cd /root/kafka/kafka_2.11-1.0.0/config
	vim server.properties
	node-1 上 broker.id=1
	node-2 上 broker.id=2

	 
	5.启动kafka 
	  5.1启动zookeeper
	   cd /root/kafka/kafka_2.11-1.0.0/bin
		./zookeeper-server-start.sh /root/kafka/kafka_2.11-1.0.0/config/zookeeper.properties

	  5.2 启动kafka
	  ./kafka-server-start.sh /root/kafka/kafka_2.11-1.0.0/config/server.properties 2>&1 &

      5.3 启动kafka生产者
      ./kafka-console-producer.sh --broker-list master:9092 --topic kafka_test

      5.4 启动kafka消费者
	 # --from-beginning 如果consumer之前没有建立offset 则从producer最开始的数据读取
       ./kafka-console-consumer.sh --bootstrap-server master:9092 --topic kafka_test --from-beginning

	  5.5 查看当前的topic
	  ./kafka-topics.sh --list --zookeeper master:2181

  	  5.6 创建一个topic
	 # 创建一个有1个partition、1个副本的 topic
	  ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka_test

	  	  
------------------------------------------------------------------------------------------
                                      [ 安装sqoop ]
------------------------------------------------------------------------------------------	  
	  
	  1.下载
	  http://mirrors.hust.edu.cn/apache/sqoop/1.4.7
	  
	  2.解压
	  tar -zxvf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
	  
	  3.放jar包
	  将 mysql-connector-java-5.1.44-bin.jar 包放到/root/sqoop/sqoop-1.4.7.bin__hadoop-2.6.0/lib
	  
	  4.配置环境变量
	  export SQOOP_HOME=/root/sqoop/sqoop-1.4.7.bin__hadoop-2.6.0
	  
	  5.查看sqoop是否安装成功
	  sqoop help
	  
	  
	  
	  
	  
	  