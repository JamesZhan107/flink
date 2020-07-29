import unittest
from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf, ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkBlinkStreamTableTestCase


class UserDefinedTableFunctionMLTest(object):
    def test_table_function(self):
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])

        self.t_env.register_table_sink("Results", table_sink)

        self.t_env.register_function(
            "multi_emit", udtf(MultiEmit(), result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()]))

        self.t_env.register_function("condition_multi_emit", condition_multi_emit)

        self.t_env.register_function(
            "multi_num", udf(MultiNum(), result_type=DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        t = t.join_lateral("multi_emit(a, multi_num(b)) as (x, y)") \
            .left_outer_join_lateral("condition_multi_emit(x, y) as m") \
            .select("x, y, m")

        t.insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()

        self.assert_equals(actual,
                           ["1,0,null", "1,1,null", "2,0,null", "2,1,null", "3,0,0", "3,0,1",
                            "3,0,2", "3,1,1", "3,1,2", "3,2,2", "3,3,null"])


class PyFlinkBlinkStreamUserDefinedFunctionMLTests(UserDefinedTableFunctionMLTest,
                                                   PyFlinkBlinkStreamTableTestCase):
    pass


class MultiEmit(TableFunction, unittest.TestCase):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def eval(self, x, y):
        self.counter.inc(y)
        self.counter_sum += y
        self.assertEqual(self.counter_sum, self.counter.get_count())
        for i in range(y):
            yield x, i


# test specify the input_types
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()],
      result_types=DataTypes.BIGINT(), udtf_type="ml")
def condition_multi_emit(x, y):
    if x == 3:
        return range(y, x)


class MultiNum(ScalarFunction):
    def eval(self, x):
        return x * 2


class UserDefinedTableFunctionMLEasyTest(object):
    def test_table_function(self):
        self.t_env.register_function("easy_func", easy_func)

        t = self.t_env.from_elements([(1, 1), (2, 1), (3, 2)], ['a', 'b'])

        t = t.join_lateral("easy_func(a, b) as x") \
             .select("x")

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])

        self.t_env.register_table_sink("Results", table_sink)

        t.insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()

        self.assert_equals(actual, ["1", "2", "1", "2", "1", "2"])


# easy function
@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_types=DataTypes.BIGINT(), udtf_type="ml")
def easy_func(x, y):
    return range(1, 3)


class PyFlinkBlinkStreamUserDefinedFunctionMLEasyTests(UserDefinedTableFunctionMLEasyTest,
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
