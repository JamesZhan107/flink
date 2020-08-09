from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf, ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkBlinkStreamTableTestCase
import logging
import os


class TFStandaloneTest(object):
    def test_tf_standalone(self):
        self.t_env.register_function("worker", test_tf_standalone)

        t1 = self.t_env.from_elements([(1, 1)], ['a', 'b'])

        t1 = t1.join_lateral("worker(a, b) as x") \
            .select("x")

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])

        self.t_env.register_table_sink("Results", table_sink)

        t1.insert_into("Results")

        self.t_env.execute("test")


# test tf standalone
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
def test_tf_standalone(x, y):
    import tensorflow as tf
    import numpy as np
    logging.info("prepare data")
    # creat data
    # 输入值[0，1)之间的随机数
    x_data = np.random.rand(100).astype(np.float32)
    # 预测值
    y_data = x_data * 0.1 + 0.3

    # -----creat tensorflow structure strat-----
    # 构造要拟合的线性模型
    Weights = tf.Variable(tf.random.uniform([1], -1.0, 1.0))
    biases = tf.Variable(tf.zeros([1]))
    y = Weights * x_data + biases

    # 定义损失函数和训练方法
    # 最小化方差
    loss = tf.reduce_mean(tf.square(y - y_data))
    optimizer = tf.compat.v1.train.GradientDescentOptimizer(0.5)
    train = optimizer.minimize(loss)

    # 初始化变量
    init = tf.initialize_all_variables()
    # -----creat tensorflow structure end-----

    # 启动
    sess = tf.compat.v1.Session()
    sess.run(init)
    logging.info("run")

    # 训练拟合，每一步训练队Weights和biases进行更新
    for step in range(201):
        sess.run(train)
        # 每20步输出一下W和b
        if step % 20 == 0:
            logging.info(step)
            logging.info(sess.run(Weights))
            logging.info(sess.run(biases))

    return range(1, 2)


class TFStandaloneTests(TFStandaloneTest, PyFlinkBlinkStreamTableTestCase):
    pass


#################----------------------------------------#############################


class TFClusterTest(object):
    def test_tf_cluster(self):
        self.t_env.register_function("worker", test_tf_cluster_worker)

        t1 = self.t_env.from_elements([[3], [2]], ['a'])

        t1 = t1.join_lateral("worker(a) as x") \
            .select("x")

        # register ps function by @, can not stop it
        # self.t_env.register_function("ps", test_tf_cluster_ps)

        # register ps function by class, can stop it
        self.t_env.register_function("ps", udtf(tf_udtf(), input_types=DataTypes.BIGINT(),
                                                result_types=DataTypes.BIGINT(), udtf_type="ml"))

        my_source_ddl2 = """
                            create table mySource2 (
                                num1 INT
                            ) with (
                                'connector' = 'ml'
                            )
                        """

        self.t_env.execute_sql(my_source_ddl2)

        t2 = self.t_env.from_path("mySource2")

        t2 = t2.join_lateral("ps(num1) as x") \
            .select("x")

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])

        self.t_env.register_table_sink("Results", table_sink)

        t1.insert_into("Results")
        t2.insert_into("Results")

        self.t_env.execute("test")


# test function
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
def test_tf_cluster_worker(x):
    flag = True
    while flag:
        if os.path.exists(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt'):
            import tensorflow as tf
            import time
            import json

            f = open(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt')
            line = f.readline()
            if line is not None:
                info = line.split("&")
                cluster_json = json.loads(info[0])
                job_name = info[1]
                index = int(info[2])

            logging.info(info[1] + "--" + info[2] + "worker start!!!!!!!!!!!!!!!!!!!!!!!")

            def build_graph():
                global a
                i = 1
                a = tf.placeholder(tf.float32, shape=None, name="a")
                b = tf.reduce_mean(a, name="b")
                r_list = []
                v = tf.Variable(dtype=tf.float32, initial_value=tf.constant(1.0), name="v_" + str(i))
                c = tf.add(b, v, name="c_" + str(i))
                add = tf.assign(v, c, name="assign_" + str(i))
                r_list.append(add)
                global_step = tf.contrib.framework.get_or_create_global_step()
                global_step_inc = tf.assign_add(global_step, 1)
                r_list.append(global_step_inc)
                return r_list

            cluster = tf.train.ClusterSpec(cluster=cluster_json)
            server = tf.train.Server(cluster, job_name=job_name, task_index=index)
            sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                                         device_filters=["/job:ps", "/job:worker/task:%d" % index])
            t = time.time()
            with tf.device(
                tf.train.replica_device_setter(worker_device='/job:worker/task:' + str(index), cluster=cluster)):
                r_list = build_graph()
                hooks = [tf.train.StopAtStepHook(last_step=2)]
            with tf.train.MonitoredTrainingSession(master=server.target, config=sess_config,
                                                   checkpoint_dir="/var/tmp/" + str(t),
                                                   hooks=hooks) as mon_sess:
                while not mon_sess.should_stop():
                    logging.info(mon_sess.run(r_list, feed_dict={a: [1.0, 2.0, 3.0]}))
            flag = False
    return range(1, 2)


class tf_udtf(TableFunction):
    def open(self, function_context):
        self.flag = True
        pass

    def ps_func(self):
            f = open(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt')
            line = f.readline()
            import json
            if line is not None:
                info = line.split("&")
                cluster_json = json.loads(info[0])
                job_name = info[1]
                index = int(info[2])

            import tensorflow as tf
            cluster = tf.train.ClusterSpec(cluster=cluster_json)
            server = tf.train.Server(cluster, job_name=job_name, task_index=index)
            server.join()

    def eval(self, x):
        if self.flag & os.path.exists(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt'):
            logging.info("start son process!!!!!!!!!!")
            import multiprocessing
            p = multiprocessing.Process(target=self.ps_func)
            p.start()
            self.flag = False
        return range(1, 2)


@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
def test_tf_cluster_ps(x):
    if os.path.exists(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt'):
        f = open(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt')
        line = f.readline()
        import json
        if line is not None:
            info = line.split("&")
            cluster_json = json.loads(info[0])
            job_name = info[1]
            index = int(info[2])

        import tensorflow as tf
        logging.info(info[1] + "--" + info[2] + "start~~~~~~~~~~~~~")
        cluster = tf.train.ClusterSpec(cluster=cluster_json)
        server = tf.train.Server(cluster, job_name=job_name, task_index=index)
        server.join()
    return range(1, 2)


class TFClusterTests(TFClusterTest, PyFlinkBlinkStreamTableTestCase):
    pass

#################----------------------------------------#############################


class MLFrameworkMutiSourceTest(object):
    def test_tf_cluster(self):
        self.t_env.register_function("worker", easy_func)

        t1 = self.t_env.from_elements([[3], [2], [3], [2], [3], [2]], ["a"], False)

        t1 = t1.join_lateral("worker(a) as x") \
            .select("x")

        self.t_env.register_function("ps", udtf(test_process(), input_types=DataTypes.BIGINT(),
                                                result_types=DataTypes.BIGINT(), udtf_type="ml"))

        my_source_ddl2 = """
                            create table mySource2 (
                                num1 INT
                            ) with (
                                'connector' = 'ml'
                            )
                        """

        self.t_env.execute_sql(my_source_ddl2)

        t2 = self.t_env.from_path("mySource2")

        t2 = t2.join_lateral("ps(num1) as x") \
            .select("x")

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])

        self.t_env.register_table_sink("Results", table_sink)

        t1.insert_into("Results")
        t2.insert_into("Results")

        self.t_env.execute("test")


# easy function
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT(), udtf_type="ml")
def easy_func(x):
    logging.info("--------------------source send num is" + str(x))
    if os.path.exists(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt'):
        import tensorflow as tf
        import time
        import json
        f = open(os.environ['BOOT_LOG_DIR'] + '/clusterInfo.txt')
        line = f.readline()
        if line is not None:
            info = line.split("&")
            job_name = info[1]
            logging.info(info[1] + "--" + info[2] + "--------------------")
            logging.info(os.getpid())
        if job_name == "worker":
            time.sleep(1)
    return range(1, 2)


class test_process(TableFunction):

    def open(self, function_context):
        self.flag = True

    def worker(self):
        """worker function"""
        while True:
            with open("/tmp/bbb", "a") as f:
                f.write("run")

    def eval(self, x):
        logging.info("father--------------" + str(os.getpid()))
        if self.flag:
            import multiprocessing
            p = multiprocessing.Process(target=self.worker)
            p.start()
            self.flag = False
        return range(1, 2)


class MLFrameworkMutiSourceTests(MLFrameworkMutiSourceTest,
                                 PyFlinkBlinkStreamTableTestCase):
    pass


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
