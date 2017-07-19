该同步应用程序由两部分组成：同步服务端（oracle-server目录下），同步客户端（oracle-client目录下）
同步服务端获取数据库的DML操作，并保存至服务端的队列中。客户端从服务队列中获取DML数据，并将数据还原至目标数据库中，下面假设将数据库databaseA的schmaA的表tableA的数据同步到databaseB的schmaB的表tableB的数据同步到

服务端配置
	数据源配置
		数据源配置文件为~/oracle-server/conf/configs.xml,结构如下：
			<configs>
				<oracle>
					<driver>oracle.jdbc.driver.OracleDriver</driver>
					<url>jdbc:oracle:thin:@ip:port:sid</url>
					<!-- do not change this -->
					<cdcuser>CDCUSER</cdcuser>
					<password>password</password>
					<!-- do not change this -->
					<tablespace>ts_cdc</tablespace>
					<datafile>/your/tablespace/file/dir/cdcuser.dbf</datafile>
					<file-size>2048m</file-size>
					<tempspace>cdcusertemp</tempspace>
					<tempfile>/your/tablespace/file/dir/cdcusertemp.dbf</tempfile>
					<temp-size>2048m</temp-size>
					<change-set>set_cdc</change-set>
					<dba-account>dba_account</dba-account>
					<dba-password>dba_password</dba-password>
					<schema name="schemaA" password="schema_password">
						<table name="tableA1"></table>
						<table name="tableA2"></table>
					</schema>
				</oracle>
			</configs>
		
	消息队列配置
		数据源配置文件为~/oracle-server/conf/activemq.xml。注意修改activemq的端口，不要和其他应用发生冲突。
	
	web容器配置
		web容器配置文件为~/oracle-server/conf/jetty.xml,注意修改jetty服务器的端口，不要和其他应用发生冲突。
		
客户端配置
	客户端配置文件为~/oracle-client/conf/configs.xml,结构如下：
	<configs>
		<oracle>
			<driver>oracle.jdbc.driver.OracleDriver</driver>
			<url>jdbc:oracle:thin:@ipB:port:sid</url>
			<!-- do not change this -->
			<cdcuser>CDCUSER</cdcuser>
			<password>password</password>
			<!-- do not change this -->
			<tablespace>ts_cdc</tablespace>
			<datafile>/your/tablespace/file/dir/cdcuser.dbf</datafile>
			<file-size>2048m</file-size>
			<tempspace>cdcusertemp</tempspace>
			<tempfile>/your/tablespace/file/dir/cdcusertemp.dbf</tempfile>
			<temp-size>2048m</temp-size>
			<change-set>set_cdc</change-set>
			<dba-account>dba_account</dba-account>
			<dba-password>dba_password</dba-password>
			
			<node ip="oracle-server_ip" port="oracle-server_activemq_port">
				<schema remote="schemaA" >
					<table remote="tableA1" localSchema="schemaB" localTable="tableB1"></table>
					<table remote="tableA2" localSchema="schemaB" localTable="tableB2"></table>
				</schema>
			</node>
		</oracle>
	</configs>
	其中<table>标签的localSchema属性缺失值为上级标签<schema>的remote属性的值，localTable属性缺失值为当前标签的remote属性的值。
	
如果需要双向同步，先将服务端和客户端应用程序反向复制一份，配置文件也同样反向配置一边即可

启动 
	需先启动服务端，再启动客户端。进入~/oracle-server/目录，运行startup.sh(Linux) 或startup.bat(windows),启动服务端。
	进入~/oracle-client/目录，运行startup.sh(Linux) 或startup.bat(windows),启动客户端。
	
停止
	查找操作系统正在运行duplex-oracle-server.jar、duplex-oracle-client.jar的进程号，kill对应的进程。	
