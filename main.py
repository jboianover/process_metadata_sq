import pandas as pd
import os
import sys
from bin.functions import groupby_clause, where_clause, select_clause, from_clause, header, parse_arguments, handler_sq

# Sample Arguments #
#arg = ['-p', 'trafico_actuaciones_f_tr_actuacion_detallada',
#       '-d', 'C:\\Users\\Maximiliano Bloisen\\Desktop\\altamira\\py',
#       '-f', 'process_metadata_actuaciones.xlsx',
#       '-o', 'process.sql',
#      ]

#process_name, chdir, file_name, output_file = parse_arguments(arg)

process_name, chdir, file_name, output_file = parse_arguments(sys.argv[1:])

#process_name = 'tc_titulares'
#chdir = 'C:\\Users\\Jonathan Boianover\\Desktop\\'
#file_name = 'process_metadata_reglas.xls'
#output_file = 'groupby_output.txt'

pm = pd.read_excel(os.path.join(chdir, file_name), sheet_name=0, header=0, names=None, index_col=None,
                                 convert_float=True, converters={'error_code' : str})

file = open(os.path.join(chdir, output_file), 'w', encoding='utf8')

pm = pm.loc[(pm['indicador'] == process_name) & (pm['active_flg'] == 'Y')]

indicadores = pm['indicador'].drop_duplicates().astype(str).tolist()

for indicador in indicadores:

    process_metadata = pm.loc[(pm['indicador'] == indicador)]
    process_table = process_metadata['dest_table'].drop_duplicates().dropna().reset_index(drop=True)
    process_table = process_table[0]

    if process_metadata['sq_flg'].str.contains('Y').any():
        subquery_df = process_metadata.loc[process_metadata['sq_flg'] == 'Y']
        dict_sq = handler_sq(subquery_df)
    else:
        dict_sq = {}

    # HEADER
    header_query = header(process_table)

    # SELECT
    process_metadata_query = process_metadata.loc[process_metadata['sq_flg'] != 'Y'].reset_index(drop=True)
    select_query = select_clause(process_metadata_query)

    # FROM
    from_query = from_clause(process_metadata_query, dict_sq)

    # WHERE
    process_metadata_where = process_metadata_query.loc[(process_metadata_query['where_value_a'].notnull()
                                                         | process_metadata_query['where_value_b'].notnull())]\
        .reset_index(drop=True)
    where_query = where_clause(process_metadata_where, dict_sq)

    # GROUP
    groupby_query = groupby_clause(process_metadata_query)

    sql_final = ''.join([select_query, '\n', from_query, where_query, '\n', groupby_query, '---------------------\n\n'])

    file.writelines(sql_final)

file.close()
