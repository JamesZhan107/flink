# 抽象添加角色方法，通过传入执行环境、角色名、遍历顺序，创建出单条执行流
from pyflink.table import DataTypes
from pyflink.testing import source_sink_utils


# def add_role(t_env, func, name, n):
#     self.t_env.register_function(name, func)

#     my_source_ddl = """
#                 create table mySource_n (
#                     num1 INT,
#                     num2 INT
#                 ) with (
#                     'connector' = 'ml'
#                 )
#             """
#
#     t_env.execute_sql(my_source_ddl)
#
#     t = t_env.from_path("mySource_"+n).join_lateral(name + "(num1, num2) as x") \
#         .select("x")
#
#     table_sink = source_sink_utils.TestAppendSink(
#         ['a'],
#         [DataTypes.BIGINT()])
#
#     t_env.register_table_sink("Results", table_sink)
#
#     t.insert_into("Results")
