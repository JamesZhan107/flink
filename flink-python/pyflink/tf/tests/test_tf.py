# 用户层面，执行tf训练
from pyflink.table import DataTypes
from pyflink.table.udf import udtf


# def test():
#     t_env = ...
#     config.add();
#     config.add();
#     add_workflow(t_env, func, config)
#     t_env.execute
#
# # easy function
# @udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
# def func(x, y):
#     return range(1, 3)
