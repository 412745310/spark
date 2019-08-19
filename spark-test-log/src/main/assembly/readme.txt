1.启动hadoop
2.启动spark
3.启动kafka
4.启动flume
nohup bin/flume-ng agent -c conf -f conf/taildir-kafka.conf -n agent1 -Dflume.root.logger=INFO,console &
5.执行mysql初始化脚本
6.提交spark任务
./spark-submit --master yarn-cluster ../lib/spark-test-log-0.0.1-SNAPSHOT.jar
7.测试
echo textlog >> 1.log
查看数据库表以及日志打印正确性