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


def header(process_table):

    header_query = (''.join(['insert into ', process_table, '\n']))

    return header_query


def select_clause(tables):
    select = (''.join(['select', '\n']))
    tables = tables.fillna('')
    tables = tables.loc[(tables['column_name'] != '')].reset_index(drop=True)
    n_rows = len(tables)

    tables = tables.fillna('')
    i = 0
    for index, row in tables.iterrows():
        if row['agg_flg'] != 'N':
            if row['column_custom_flg'] == 'Y':
                select += (''.join([row['column_name']]))
            else:
                select += (''.join([row['column_source'], '.', row['column_name']]))
            if not pd.isna(row['column_alias']):
                select += (''.join([' ', str(row['column_alias'])]))
        else:
            if row['column_custom_flg'] == 'Y':
                select += (''.join([row['agg_operation'], '(', row['column_name'], ')']))
            else:
                select += (''.join([row['agg_operation'], '(', row['column_source'], '.', row['column_name'], ') ',
                                    row['column_alias']]))

        if i != n_rows - 1:
            select += (''.join([',', '\n']))
        else:
            select += '\n'
        i += 1
    return select


def from_clause(process_metadata_from, dict_sq):

    process_metadata = process_metadata_from.fillna('').reset_index(drop=True)

    table_a_b = process_metadata[['table_a', 'table_b']].drop_duplicates().reset_index(drop=True)

    #table_a_b = table_a_b.loc[(str(table_a_b['table_a']) != '' & str(table_a_b['table_b']) != '')]

    if process_metadata['sq_from_flg_a'].iloc[0] != 'Y':

        from_sq = (''.join(['', 'from ', process_metadata.loc[0, 'table_a'], '\n']))
    else:

        from_sq = (''.join(['', 'from (', dict_sq[process_metadata.loc[0, 'table_a']], ')\n']))

    table_b_aux = ''

    for index, row in table_a_b.iterrows():
        if (str(row['table_a']) != '') & (str(row['table_b']) != ''):
            from_columns = process_metadata.loc[((process_metadata['table_a'] == row['table_a']) &
                                                (process_metadata['table_b'] == row['table_b']))].reset_index(drop=True)

            join_columns = from_columns[['sq_from_flg_a', 'table_a', 'sq_from_flg_b', 'table_b', 'how', 'fk',
                                         'criteria', 'key']].dropna().reset_index(drop=True)

            if table_b_aux != row['table_b']:
                if join_columns.loc[0, 'sq_from_flg_b'] != 'Y':
                    from_sq += (''.join(['', str(join_columns.loc[0, 'how']), ' join ',
                                         str(join_columns.loc[0, 'table_b']), ' on\n']))

                else:
                    from_sq += (''.join(['', str(join_columns['how']), ' join (', str(dict_sq[table_a_b.loc[0, 'table_b']]),
                                         ') on\n']))
            table_b_aux = row['table_b']

            for index_i, row_i in join_columns.iterrows():

                from_sq += (''.join(['(', row_i['table_a'], '.', row_i['fk'], ' ', row_i['criteria'], ' ',
                                     row_i['table_b'], '.', row_i['key'], ')']))
                if (index_i != (len(join_columns)-1)) and (table_b_aux == row_i['table_b']):
                    from_sq += ' and\n'
                else:
                    from_sq += '\n'
    return from_sq


def where_clause(process_metadata, dict_sq):

    dim_columns = process_metadata[['sq_where_flg', 'where_value']].drop_duplicates().reset_index(drop=True)
    where_query = 'where\n'

    max_lines = len(dim_columns)
    i = 0

    for index, row in dim_columns.iterrows():

        if row['sq_where_flg'] == 'N':
            if not pd.isnull(row['where_value']):
                where_query += str(row['where_value'])
        else:
            where_value = str(row['where_value'])
            where_value_replace = where_value

            for key in dict_sq:

                where_value_replace = str.replace(where_value_replace, key, dict_sq[key])

            where_query += where_value_replace

        if i != max_lines - 1:
            where_query += ' and\n'
        else:
            where_query += '\n'
        i += 1

    return where_query


def groupby_clause(process_metadata):
    process_metadata = process_metadata.fillna('')
    if process_metadata['agg_flg'].str.contains('N').any():
        dim_columns = process_metadata[['column_source', 'column_name', 'column_custom_flg', 'agg_flg',
                                        'agg_operation']].drop_duplicates().reset_index(drop=True)

        dim_tables_filtered = dim_columns.loc[(dim_columns['agg_flg'] == 'Y') & (dim_columns['column_name'] != '')]\
            .reset_index(drop=True)
        cant_columnas_group = len(dim_tables_filtered)
        groupby_query = (''.join(['group by', '\n']))

        i = 1

        for index, row in dim_tables_filtered.iterrows():

            if row['column_custom_flg'] == 'N':
                groupby_query += (''.join([row['column_source'], '.', row['column_name']]))
            else:
                groupby_query += (''.join([row['column_name']]))

            if i < cant_columnas_group:
                groupby_query += (''.join([',', '\n']))
            else:
                groupby_query += ';\n'

            i += 1
    else:
        groupby_query = ''
    return groupby_query


def handler_sq(process_metadata_sq):

    df_sq = process_metadata_sq['subquery_name'].drop_duplicates().dropna().reset_index(drop=True)

    lista_sq = df_sq.values.tolist()

    dict_sq = {}

    i = 0
    if len(lista_sq) != 0:
        for item in lista_sq:

            process_metadata = process_metadata_sq.loc[(process_metadata_sq['subquery_name'] == lista_sq[i])]
            # SELECT

            subquery = select_clause(process_metadata)

            # FROM

            process_metadata_from = process_metadata.loc[(process_metadata['table_a'].notnull())].reset_index(drop=True)

            subquery += (''.join([' ', from_clause(process_metadata_from, dict_sq)]))

            # WHERE
            process_metadata_where = process_metadata.loc[(process_metadata['where_value'].notnull())]\
                .reset_index(drop=True)

            subquery += (''.join([' ', where_clause(process_metadata_where, dict_sq)]))

            # GROUP BY
            subquery += groupby_clause(process_metadata)

            dict_sq[lista_sq[i]] = (''.join([' ', '(', subquery, ')']))

            i += 1
    return dict_sq
