import pandas as pd
import numpy as np

def aggregateColumns(alg_cols, input_cols_obj, input_df_obj):
    """Aggregate (sum) values of columns based on instructions in the generalizations.xlsx file"""

    assert type(input_df_obj) == type(input_cols_obj)

    if isinstance(input_df_obj, pd.Series):
        assert len(alg_cols) >= len(input_cols_obj)
        for cols_ind, input_cols in enumerate(input_cols_obj):
            input_cols = [input_col.strip() for input_col in input_cols.split(";")]
            input_df_obj[alg_cols[cols_ind]] = input_df_obj[input_cols].sum(axis=1)
    else:
        assert len(input_df_obj) == len(input_cols_obj)
        for input_ind, input_cols in enumerate(input_cols_obj):
            assert len(alg_cols) >= len(input_cols)
            for cols_ind, cols_str in enumerate(input_cols):
                cols = [col_str.strip() for col_str in cols_str.split(";")]
                input_df_obj[input_ind][alg_cols[cols_ind]] = input_df_obj[input_ind][cols].sum(axis=1).copy()
            for cols_ind, cols_str in enumerate(input_cols):
                cols = [col_str.strip() for col_str in cols_str.split(";")
                        if col_str.strip() != alg_cols[cols_ind]]
                input_df_obj[input_ind] = input_df_obj[input_ind].drop(cols, axis=1).copy()

    return input_df_obj

def mappingValCategories(alg_groups, input_groups_obj, input_df_obj):
    """Create groups of values of columns based on instructions in the generalizations.xlsx file"""

    assert type(input_df_obj) == type(input_groups_obj)

    if isinstance(input_df_obj, pd.Series):
        assert len(alg_groups) + 1 == len(input_groups_obj)
        input_df_obj = input_df_obj.astype(input_groups_obj.dtype)
        input_df_obj = pd.cut(input_df_obj, input_groups_obj, labels=alg_groups)
    else:
        for input_ind, input_df in enumerate(input_df_obj):
            assert len(alg_groups) + 1 == len(input_groups_obj[input_ind])
            input_df_obj[input_ind] = input_df_obj[input_ind].astype(input_groups_obj[input_ind].dtype)
            input_df_obj[input_ind] = pd.cut(input_df, input_groups_obj[input_ind], labels=alg_groups)

    return input_df_obj

def mappingStdCategories(alg_groups, input_groups_obj, input_df_obj):
    """Change values of columns based on instructions in the generalizations.xlsx file"""

    assert type(input_df_obj) == type(input_groups_obj)

    if isinstance(input_df_obj, pd.Series):
        assert len(alg_groups) >= len(input_groups_obj)
        mapping = dict()
        for vals_ind, input_vals in enumerate(input_groups_obj):
            try:
                for input_val in input_vals.split(','):
                    input_val = input_val.strip()
                    if mapping.__contains__(input_val):
                        mapping[input_val].append(alg_groups[vals_ind])
                    else:
                        mapping.update({input_val: [alg_groups[vals_ind]]})
            except AttributeError:
                # nan
                skip = 1
        for key in mapping:
            mapping.update({key: tuple(mapping[key])})

        input_df_obj = input_df_obj.map(mapping)
        if len(input_df_obj.loc[input_df_obj.str.len() > 1]) == 0:
            input_df_obj = input_df_obj.apply(lambda x: ', '.join([str(i) for i in x]))
    else:
        assert len(input_df_obj) == len(input_groups_obj)
        for input_ind, input_groups in enumerate(input_groups_obj):
            assert len(alg_groups) >= len(input_groups)
            mapping = dict()
            for vals_ind, input_vals in enumerate(input_groups):
                try:
                    for input_val in input_vals.split(','):
                        input_val = input_val.strip()
                        if mapping.__contains__(input_val):
                            mapping[input_val].append(alg_groups[vals_ind])
                        else:
                            mapping.update({input_val: [alg_groups[vals_ind]]})
                except AttributeError:
                    # nan
                    skip = 1

            for key in mapping:
                mapping.update({key: tuple(mapping[key])})

            input_df_obj[input_ind] = input_df_obj[input_ind].map(mapping)
            if len(input_df_obj[input_ind].loc[input_df_obj[input_ind].str.len() > 1]) == 0:
                input_df_obj[input_ind] = input_df_obj[input_ind].apply(lambda x: ', '.join([str(i) for i in x]))

    return input_df_obj

def calcCityHTSJourneyTimes(trip_data):
    """Journey main mode estimated by the longest trip (in time)"""
    journeyTimes = pd.DataFrame(columns=['on foot', 'bike', 'city public transport',
                                         'bus (except city public transport)',
                                         'train (except city public transport)', 'auto-driver', 'auto-passenger',
                                         'other', 'Not identified'])
    journeyTimes['on foot'] = trip_data['TimeFoot'].astype(np.int).sum()
    journeyTimes['bike'] = trip_data['TimeBike'].astype(np.int).sum()
    journeyTimes['city public transport'] = trip_data['TimeTownBus', 'TimeTrolleyBus'].astype(np.int).sum()
    journeyTimes['bus (except city public transport)'] = trip_data['TimeRegionalBus'].astype(np.int).sum()
    journeyTimes['train (except city public transport)'] = trip_data['TimeTrain'].astype(np.int).sum()
    journeyTimes['auto-driver'] = trip_data['TimeDriverCar'].astype(np.int).sum()
    journeyTimes['auto-passenger'] = trip_data['TimePassengerCar', 'TimeTaxi'].astype(np.int).sum()
    journeyTimes['other'] = trip_data['TimeMoto', 'TimeOther'].astype(np.int).sum()
    columns = ['1', '2', '3', '4', '5', '6', '7', '8', '999']
    journeyTimes = pd.DataFrame(columns=columns)

    return journeyTimes

def toXML(df, filename=None, mode='w'):
    """Save data into XML file"""

    def row_to_xml(row):
        for i, col_name in enumerate(row.index):
            if i == 0:
                fields = '{0}=\"{1}\"'.format(col_name, row.iloc[i])
            else:
                fields += ' {0}=\"{1}\"'.format(col_name, row.iloc[i])
        xml = '    <row ' + fields + ' </row>'

        return xml
    res = '<data>\n' + '\n'.join(df.apply(row_to_xml, axis=1)) + '\n</data>'

    if filename is None:
        return res
    with open(filename, mode, encoding="utf-8") as f:
        f.write(res)