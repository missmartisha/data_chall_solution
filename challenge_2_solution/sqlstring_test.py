import unittest
import sqlstring as ss

class TestProcess(unittest.TestCase):
    def setUp(self):

        self.line_dict = {
            "line": ("      c_2_g_start_date_c as year_start_date", "c_2_g_start_date_c"),
            "line_dbt": ("      {{ null_if ('name') }} as year_name", "name"),
            "source": (""" {{ source('financialforce', 'c_2_g_coda_year_c') }}
        where is_deleted = FALSE""", "c_2_g_coda_year_c"),
        }

        self.kw_res = ('c_2_g_coda_year_c', 
        ['id',
        'name',
        'c_2_g_status_c',
        'c_2_g_start_date_c',
        'c_2_g_end_date_c',
        'c_2_g_owner_company_c'])

    def test_process_other(self):
        inp, res = self.line_dict['line']
        self.assertEqual(ss.process_line(inp),res)

    def test_process_dbt(self):
        inp, res = self.line_dict['line_dbt']
        self.assertEqual(ss.process_dbt(inp),res)

    def test_process_source(self):
        inp, res = self.line_dict['source']
        self.assertEqual(ss.process_source(inp),res)

    def test_process_line(self):
        for inp, res in [self.line_dict['line'],self.line_dict['line_dbt']]:
             self.assertEqual(ss.process_line(inp),res)
    
    def test_process_file(self):
        kw_dict = {}
        self.assertEqual(ss.process_file('test_query.sql', kw_dict), self.kw_res)

if __name__ == '__main__':
    unittest.main()