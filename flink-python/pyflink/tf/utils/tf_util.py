from pyflink.dl.job import DLUtils
from pyflink.table import DataTypes
from pyflink.table.udf import udtf
from pyflink.testing import source_sink_utils


class TFUtils(DLUtils):
    def add_workflow(self, t_env, func, config):

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])
        t_env.register_table_sink("Results", table_sink)

        for name, num in vars(config).items():
            self.add_role(t_env, func, name)

    def add_role(self, t_env, func, name):

        if name == "worker":
            t_env.register_function(name, udtf(func(), input_types=DataTypes.BIGINT(),
                                    result_types=DataTypes.BIGINT(), udtf_type="ml"))

            # batch mode
            t1 = t_env.from_elements([[3.0], [3.0]], ['a'])
            # stream mode
            # t1 = t_env.from_elements([[3.0], [3.0], [4.0], [4.0], [5.0], [5.0]], ['a'])

            t1 = t1.join_lateral("worker(a) as x") \
                .select("x")

            t1.insert_into("Results")

        else:
            t_env.register_function(name, udtf(func(), input_types=DataTypes.BIGINT(),
                                    result_types=DataTypes.BIGINT(), udtf_type="ml"))

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

