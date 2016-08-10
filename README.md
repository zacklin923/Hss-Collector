HSS-Collector
========
基于Akka Cluster2.4.8和Akka Stream2.4.8以及Redis和Kafka-0.9.0.1的ftp日志文件采集和记录入库集群
支持节点自动发现以及roundRobin负载分配, 支持节点故障任务自动转移, 支持分布式采集
支持ftp目录为二级目录,日志文件为包含header的RFC.RFC4180的csv文件的kafka入库

## redis记录格式
记录总共有多少目录:

``hss:dirs -> set(dir)``

记录该目录最后一个文件的名字,新添加的文件必须大于该文件名字:

``hss:dir:SZHHSS07/188.2.138.22:lastfile -> string(filename)``

记录该目录未处理的文件名,已处理的自动从集合删除:

``hss:dir:SZHHSS07/188.2.138.22:files -> set(filename)``

## 集群名称: hss-cluster
## CollectorMaster: (collector-master)
用于获取所有目录列表,把目录列表写入到redis,并且把目录列表分发到各个CollectorRouter
## CollectorRouter: (collector-router)
作为ListFileWorker的封装router,分发文件列表任务到ListFileWorker
## ListFileWorker: (listfile-router)
获取目录下的文件列表,并且把新的文件列表写入到redis以及kafka
## GetFileWorker: (getfile-worker)
从kafka读取文件列表,并把文件ftp下载下来,入库到kafka的记录topic中

## 运行说明:
先启动master,用于获取ftp目录下的文件夹. master可以开多个集群进程( 2551 和 2552 端口为akka集群的seed nodes端口, 务必先启动): 

``cmgd.zenghj.hss.HssCollector -s master -p 2551``

再启动router,用于获取ftp目录下的文件清单. router可以开多个集群进程,提升新文件发现效率:

``cmgd.zenghj.hss.HssCollector -s router -p 2552``

最后启动worker,获取ftp文件,并且把文件进行记录入库. worker可以开多个集群进程, 提升入库效率:

``cmgd.zenghj.hss.HssCollector -s worker -p 2553``

## benchmark
单机运行,单kafka节点,单redis节点,双线程afkaReactiveStream
5088项日志, 大小2.4GB, 记录数2732349, 入库时间15分钟, 入库效率3036rps(3036记录每秒)

