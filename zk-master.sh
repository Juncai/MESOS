$ZK_HOME/zkServer.sh start

$MESOS_HOME/bin/mesos-master.sh --ip=127.0.0.1 --port=10001 --work_dir=/tmp/mesos_0 --zk=zk://127.0.0.1:2181/mesos --quorum=1
$MESOS_HOME/bin/mesos-master.sh --ip=127.0.0.1 --port=10002 --work_dir=/tmp/mesos_1 --zk=zk://127.0.0.1:2181/mesos --quorum=1
$MESOS_HOME/bin/mesos-master.sh --ip=127.0.0.1 --port=10003 --work_dir=/tmp/mesos_2 --zk=zk://127.0.0.1:2181/mesos --quorum=1

$MESOS_HOME/bin/mesos-slave.sh --master=zk://127.0.0.1:2181/mesos

$MESOS_HOME/src/test-framework --master=zk://127.0.0.1:2181/mesos

# $MESOS_BAK/bin/mesos-master.sh --ip=127.0.0.1 --port=10001 --work_dir=/tmp/mesos --zk=zk://127.0.0.1:2181/mesos --quorum=3
# $MESOS_BAK/bin/mesos-master.sh --ip=127.0.0.1 --port=10002 --work_dir=/tmp/mesos --zk=zk://127.0.0.1:2181/mesos --quorum=3
$ZK_HOME/zkCli.sh -server 127.0.0.1:2181

$ZK_HOME/zkServer.sh stop
