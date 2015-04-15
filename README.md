# export_hbase_table
export hbase table data to csv file use spark framework

此工具可以将hbase的表中相应scan到的数据导出为csv格式文件


提交命令

$spark-submit  --jars $(echo /opt/cloudera/parcels/CDH/lib/hbase/lib/*.jar | tr ' ' ',')  --class io.github.zerix.utils.hbase.export_hbase_table --master spark://master_ip:7077 --driver-memory 8G --driver-cores 8 --executor-memory 8G --total-executor-cores 32  the_path_of_export_hbase_table.jar spark://master_ip:7077 zookeeper_quorum(eg:slave160:2181) 2181 zookeeper_host hbase_master_ip:60000 32 hbase_table_to_export hbase_scan_start_rowkey(eg:ap-dns:1427817600) hbase_scan_stop_rowkey /hbase_backup_path/source_table 1 2 rowkey

说明：
table_name:要导出的hbase表名
start_row:要导出的数据的起始rowkey
stop_row:要导出的数据的结束rowkey(此处start和stop和hbase的scan命令中的STARTROW和
STOPROW意义一样)
output_file_path:hbase中数据导出的路径
task_type: 1是指可以按数据表中的某个字段归类生成文件，比如按表字段中机器IP导出各自的hbase数据
		   2是导出所有的数据，混合在一起，不分类
group_field_index:当task_type为1时有效，指按照hbase中第几个字段值来分类，此顺序和hbase scan的
字段顺序一致
prefix_field:增加到导出数据行第一个字段命令，如果为rowkey字符串，将会把hbase的rowkey作为第一个
字段输出。（主要用来作为数据说明使用）

