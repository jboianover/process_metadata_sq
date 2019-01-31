# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
import getopt


def parse_arguments(arguments):
    try:
        opts, args = getopt.getopt(arguments, 'p:d:f:o:', ['process_name=', 'chdir=', 'file_name=', 'output_file='])
    except getopt.GetoptError as err:
        print(str(err))
        print('Options and Arguments:\n\
                -p | --process_name= Nombre del Proceso\n\
                -d | --chdir=        Nombre del PATH donde se ubica el process_metadata\n\
                -f | --file_name=    Nombre del archivo process_metadata\n\
                -o | --output_file=  Nombre del SQL de salida')
        sys.exit()

    for k, v in opts:
        if k in ('-p', '--process_name'):
            process_name = v
        elif k in ('-d', '--chdir'):
            chdir = v
        elif k in ('-f', '--file_name'):
            file_name = v
        elif k in ('-o', '--output_file'):
            output_file = v

    return process_name, chdir, file_name, output_file


def header(file, process_table):
    file.writelines(''.join(['insert into ', process_table, '\n']))
    return 0


def select_clause(file, tables):
    file.writelines(''.join(['select', '\n']))
    tables = tables.reset_index(drop=True)
    n_rows = len(tables.index)

    for index, row in tables.iterrows():
        if row['custom_column_flag'] == 'Y':
            file.writelines(''.join([row['column']]))
            if index != n_rows - 1:
                file.writelines(''.join([',', '\n']))
        elif pd.isna(row['agg_operation']):
            #   file.writelines(''.join([row['column_source'], '.', row['column']]))

            file.writelines(''.join(['case when ', str(row['column_source']), '.', str(row['column']), ' ',
                                     'is not null then', ' ', str(row['column_source']), '.', str(row['column']), ' ', 'else',
                                     ' ',
                                     str(row['default_value']), ' end']))
            if not pd.isna(row['column_alias']):
                file.writelines(''.join([' ', str(row['column_alias'])]))
            if index != n_rows - 1:
                file.writelines(''.join([',', '\n']))

        else:
            #   file.writelines(''.join([row['agg_operation'], '(', row['column_source'], '.', row['column'], ')']))

            file.writelines(''.join([row['agg_operation'], '(', 'case when ', str(row['column_source']), '.', str(row['column']), ' ',
                                     'is not null then', ' ', str(row['column_source']), '.', str(row['column']), ' ', 'else',
                                     ' ',
                                     str(row['default_value']), ' end', ')']))
            if not pd.isna(row['column_alias']):
                file.writelines(''.join([' ', row['column_alias']]))
            if index != n_rows - 1:
                file.writelines(''.join([',', '\n']))
    return 0


def from_clause(file, tables):
    fc = tables[['source_table']].drop_duplicates().dropna().reset_index(drop=True)
    file.writelines(''.join([' ', '\n', 'from ', fc.loc[0, 'source_table'], '\n']))
    seqs = tables['seq'].drop_duplicates().astype(int).tolist()
    for seq in seqs:
        from_columns = tables.loc[(tables['seq'] == seq)].reset_index(drop=True)
        tables = tables.reset_index(drop=True)
        if not pd.isna(tables.loc[0, 'dim_table']):
            file.writelines(''.join([' ', str(from_columns.loc[0, 'how']), ' join ', str(from_columns.loc[0, 'dim_table'])
                                     , ' on ']))
            join_columns = from_columns[['fk', 'key', 'criteria']].reset_index(drop=True).dropna()
            n_rows = len(join_columns.index)
            for index, row in join_columns.iterrows():
                file.writelines(''.join([' (', row['fk'], row['criteria'], row['key'], ')']))
                if index != n_rows - 1:
                    file.writelines(''.join([' and ']))
                else:
                    file.writelines('\n')
    return 0


def where_clause(file, process_metadata):

    dim_columns = process_metadata[['column_source', 'where_column', 'where_criteria', 'where_value', 'where_custom_flag']] \
        .drop_duplicates().reset_index(drop=True)

    dim_tables_filtered = dim_columns.loc[(dim_columns['where_column'].notnull() | dim_columns['where_value'].notnull())]
    max_lines = len(dim_tables_filtered)
    file.writelines('where \n')

    max_lines = len(dim_tables_filtered)

    i = 1

    for index, row in dim_tables_filtered.iterrows():
        if pd.isnull(row['where_value']):
            where_value = ''
        else:
            where_value = (''.join([' ', str(row['where_value'])]))

        if row['where_custom_flag'] == 'N':

            file.writelines(''.join([row['column_source'], '.', row['where_column'], ' ', row['where_criteria'], where_value]))
        else:
            file.writelines(''.join([where_value, '\n']))

        if i < max_lines:
            file.writelines(' and\n')
        else:
            file.writelines('\n')
        i += 1

    return 0


def groupby_clause(file, process_metadata):

    dim_columns = process_metadata[['seq', 'column_source', 'column', 'custom_column_flag', 'default_value', 'agg_flg']] \
        .drop_duplicates().reset_index(drop=True)
    dim_tables_filtered = dim_columns.loc[(dim_columns['agg_flg'] == 'Y') & (dim_columns['column'].notnull())] \
        .reset_index(drop=True)
    cant_columnas_group = len(dim_tables_filtered)
    file.writelines(''.join(['group by', '\n']))

    i = 1

    for index, row in dim_tables_filtered.iterrows():

        if row['custom_column_flag'] == 'N':
            if pd.isna(dim_tables_filtered.loc[index]['default_value']):
                file.writelines(''.join([row['column_source'], '.', row['column']]))
            else:
                file.writelines(''.join(['case when ', row['column_source'], '.', row['column'], ' ', \
                                'is not null then', ' ', row['column_source'], '.', row['column'], ' ', 'else', ' ', \
                                         str(row['default_value']), ' end']))
        else:
            file.writelines(''.join([row['column']]))

        if i < cant_columnas_group:
            file.writelines(''.join([',', '\n']))
        else:
            file.writelines(';\n')

        i += 1

    return 0
