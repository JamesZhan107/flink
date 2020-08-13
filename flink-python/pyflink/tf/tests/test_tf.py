# 用户层面，执行tf训练
import json

from pyflink.table import DataTypes
from pyflink.table.udf import udtf
from pyflink.testing.test_case_utils import PyFlinkBlinkStreamTableTestCase
from pyflink.tf.utils.tf_util import TFUtils


class TFOnFlinkTest(object):
    def test_tf_on_flink(self):
        role = RoleConfig(2, 1)
        role.worker = 2
        role.ps = 1
        tfUtil =TFUtils()
        tfUtil.add_wokeflow(self.t_env, func, role)
        self.t_env.execute("test")


class RoleConfig:
    def __init__(self, worker, ps):
        self.worker = worker
        self.ps = ps


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
