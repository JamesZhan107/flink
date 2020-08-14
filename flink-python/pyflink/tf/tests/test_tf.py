from pyflink.table import DataTypes
from pyflink.table.udf import udtf, TableFunction
from pyflink.testing.test_case_utils import PyFlinkBlinkStreamTableTestCase
from pyflink.tf.utils.tf_util import TFUtils
import logging
import os


class TFOnFlinkTest(object):
    def test_tf_on_flink(self):
        role = RoleConfig(2, 1)
        role.worker = 2
        role.ps = 1
        tfUtil = TFUtils()
        tfUtil.add_wokeflow(self.t_env, tf_stream_sample, role)
        self.t_env.execute("test")


class RoleConfig:
    def __init__(self, worker, ps):
        self.worker = worker
        self.ps = ps


class tf_batch_sample(TableFunction):
    def open(self, function_context):
        import json
        import tensorflow as tf
        self.flag = True
        self.job_name = os.environ['table.exec.job_name']
        self.index = int(os.environ['table.exec.index'])

        cluster_json = json.loads(os.environ['table.exec.cluster_info'])
        self.cluster = tf.train.ClusterSpec(cluster=cluster_json)
        self.server = tf.train.Server(self.cluster, job_name=self.job_name, task_index=self.index)

    def worker_func(self, x):
        import tensorflow as tf
        import time
        sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                                     device_filters=["/job:ps", "/job:worker/task:%d" % self.index])
        with tf.device(
            tf.train.replica_device_setter(worker_device='/job:worker/task:' + str(self.index), cluster=self.cluster)):
            '''
            v是变量, b是增量
            c = b + v, v = c, addwb = v
            '''
            b = tf.Variable(dtype=tf.float32, initial_value=tf.constant(1.0))
            v = tf.Variable(dtype=tf.float32, initial_value=tf.constant(2.0))
            c = tf.add(b, v)
            addwb = tf.assign(v, c)
            global_step = tf.contrib.framework.get_or_create_global_step()
            global_step_inc = tf.assign_add(global_step, 1)
            hooks = [tf.train.StopAtStepHook(last_step=4)]
            t = time.time()
            with tf.train.MonitoredTrainingSession(master=self.server.target, config=sess_config,
                                                   checkpoint_dir="/var/tmp/" + str(t),
                                                   hooks=hooks) as mon_sess:
                while not mon_sess.should_stop():
                    result = mon_sess.run([addwb, global_step_inc])
                    logging.info("result: " + str(result) + "index: " + str(self.index))

    def eval(self, x):
        if self.job_name == "worker":
            self.worker_func(x)

        return range(1, 2)


class tf_stream_sample(TableFunction):
    def open(self, function_context):
        import json
        import tensorflow as tf
        self.flag = True
        self.job_name = os.environ['table.exec.job_name']
        self.index = int(os.environ['table.exec.index'])

        cluster_json = json.loads(os.environ['table.exec.cluster_info'])
        self.cluster = tf.train.ClusterSpec(cluster=cluster_json)
        self.server = tf.train.Server(self.cluster, job_name=self.job_name, task_index=self.index)
        if "worker" == self.job_name:
            self.func_return = self.worker_func()

    def worker_func(self):
        import tensorflow as tf
        import time
        sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                                     device_filters=["/job:ps", "/job:worker/task:%d" % self.index])
        with tf.device(
            tf.train.replica_device_setter(worker_device='/job:worker/task:' + str(self.index), cluster=self.cluster)):
            '''
            v是变量, b是由外部传入的
            c = b + v, v = c, addwb = v
            '''
            global a
            a = tf.placeholder(tf.float32, shape=None, name="a")
            b = tf.reduce_mean(a, name="b")
            v = tf.Variable(dtype=tf.float32, initial_value=tf.constant(2.0))
            c = tf.add(b, v)
            addwb = tf.assign(v, c)

            global_step = tf.contrib.framework.get_or_create_global_step()
            global_step_inc = tf.assign_add(global_step, 1)
            t = time.time()
            return [tf.train.MonitoredTrainingSession(master=self.server.target, config=sess_config,
                                                      checkpoint_dir="/var/tmp/" + str(t)), addwb, global_step_inc]

    def eval(self, x):
        if self.job_name == "worker":
            session = self.func_return[0]
            addwb = self.func_return[1]
            global_step_inc = self.func_return[2]
            result = session.run([addwb, global_step_inc], feed_dict={a: [float(x)]})
            logging.info("result: " + str(result) + "  index: " + str(self.index))

        return range(1, 2)

    def close(self):
        self.func_return[0].close()


# easy function
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
def func(x):
    import os
    with open("/tmp/www", "a") as f:
        f.write(os.environ['table.exec.cluster_info'] + "\n")
        f.write(os.environ['table.exec.job_name'] + "\n")
        f.write(os.environ['table.exec.index'] + "\n")
    return range(1, 3)


class TFClusterTests(TFOnFlinkTest, PyFlinkBlinkStreamTableTestCase):
    pass
