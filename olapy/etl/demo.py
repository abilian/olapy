import bonobo
import os

from olapy.etl.etl import ETL

if __name__ == '__main__':

    # with extension
    dims_infos = {
        # 'dimension': ['col_id'],
        'Geography': ['geokey']
    }

    facts_ids = ['geokey', 'Day', 'City', 'Licence']

    etl = ETL(
        source_type='csv',
        facts_table='sales_facts',
        facts_ids=facts_ids,
        **dims_infos)

    for table in list(dims_infos.keys()) + [etl.facts_table]:
        # transform = Transform(dims_infos[table])
        etl.dim_first_row_headers = True
        if table == etl.facts_table:
            etl.current_dim_id_column = facts_ids
        else:
            etl.current_dim_id_column = dims_infos[table]

        graph = bonobo.Graph(
            etl.extract(os.path.join('input_demos', table + etl.get_source_extension()), delimiter=','),
            etl.transform, etl.load(table))

        bonobo.run(graph)

    # temp ( bonobo can't export (save) to path (bonobo bug)
    etl.copy_2_olapy_dir()
