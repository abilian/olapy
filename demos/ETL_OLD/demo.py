from olapy.etl.etl_OLD import run_olapy_etl

if __name__ == '__main__':
    dims_infos = {
        # 'dimension': ['col_id'],
        'Geography': ['geography_id'],
        'Product': ['product_id']
    }

    facts_ids = ['geography_id', 'product_id']
    # source_type = 'csv' | 'file' | 'db'
    run_olapy_etl(
        source_type='csv',
        dims_infos=dims_infos,
        facts_table='sales_facts',
        facts_ids=facts_ids)
