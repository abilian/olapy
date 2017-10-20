from olapy.etl.etl import run_olapy_etl

if __name__ == '__main__':
    dims_infos = {
        # 'dimension': ['col_id'],
        'Geography': ['geography_key'],
        'Product': ['product_key']
    }

    facts_ids = ['geography_key', 'product_key']
    run_olapy_etl(dims_infos=dims_infos, facts_table='sales_facts', facts_ids=facts_ids)
