# 抽象创建工作流方法，通过传入json配置字符串，遍历构造工作流
import json

from pyflink.table import DataTypes
from pyflink.testing import source_sink_utils


class TFUtils():
    def add_wokeflow(self, t_env, func, config):

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])
        t_env.register_table_sink("Results", table_sink)

        for name, num in vars(config).items():
            print(name)
            print(num)
            self.add_role(t_env, func, name, num)

    def add_role(self, t_env, func, name, num):

        if name == "worker":
            t_env.register_function(name, func)

            t1 = t_env.from_elements([[3], [2]], ['a'])

            t1 = t1.join_lateral("worker(a) as x") \
                .select("x")

            t1.insert_into("Results")

        else:
            t_env.register_function(name, func)

            my_source_ddl = """
                        create table mySource (
                            a INT
                        ) with (
                            'connector' = 'ml'
                        )
                    """

            t_env.execute_sql(my_source_ddl)

            t2 = t_env.from_path("mySource")

            t2 = t2.join_lateral("ps(a) as x") \
                .select("x")

            t2.insert_into("Results")

