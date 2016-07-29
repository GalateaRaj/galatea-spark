# galatea-spark

In order to run this POC, you need to install and run HDFS locally.

I followed these instructions:  http://toodey.com/2015/08/10/hadoop-installation-on-windows-without-cygwin-in-10-mints/ but had to tweak hdfs-site.xml. I used the following config for hdfs-site.xml (my hadoop directories on disk were c:\hadoop_data\namenode and c:\hadoop_data\datanode)

`<configuration>
  <property>
      <name>dfs.replication</name>
      <value>1</value>
  </property>
  <property>
      <name>dfs.namenode.name.dir</name>
      <value>/hadoop_data/namenode</value>
  </property>
  <property>
      <name>dfs.datanode.data.dir</name>
    <value>/hadoop_data/datanode</value>
  </property>
</configuration>`

