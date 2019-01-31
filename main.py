import pandas as pd
import os
import sys
from bin.functions import groupby_clause, where_clause, select_clause, from_clause, header, parse_arguments

# Sample Arguments #
#arg = ['-p', 'trafico_actuaciones_f_tr_actuacion_detallada',
#       '-d', 'C:\\Users\\Maximiliano Bloisen\\Desktop\\altamira\\py',
#       '-f', 'process_metadata_actuaciones.xlsx',
#       '-o', 'process.sql',
#      ]

#process_name, chdir, file_name, output_file = parse_arguments(arg)

process_name, chdir, file_name, output_file = parse_arguments(sys.argv[1:])

#process_name = 'test'
#chdir = 'C:\\Users\\Jonathan Boianover\\Desktop\\'
#file_name = 'process_metadata_galicia.xls'
#output_file = 'groupby_output.txt'

pm = pd.read_excel(os.path.join(chdir, file_name), sheet_name=0, header=0, names=None, index_col=None,
                                 convert_float=True, converters={'error_code' : str})

file = open(os.path.join(chdir, output_file), 'w', encoding='utf8')

pm = pm.loc[(pm['process_name'] == process_name) & (pm['active_flg'] == 'Y')]

batches = pm['batch'].drop_duplicates().astype(int).tolist()


for batch in batches:

    process_metadata = pm.loc[(pm['batch'] == batch)]
    process_table = process_metadata['dest_table'].drop_duplicates().dropna().reset_index(drop=True)
    process_table = process_table[0]

    # HEADER
    header(file, process_table)

    # SELECT
    select_clause(file, process_metadata)

    # FROM
    from_clause(file, process_metadata)

    # WHERE
    where_clause(file, process_metadata)

    # GROUP
    groupby_clause(file, process_metadata)

    file.writelines('---------\n\n')

file.close()