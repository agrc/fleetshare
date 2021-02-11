import arcpy
# import datetime

import pandas as pd

operator_fc = r'A:\telework_survey\wfh.gdb\operator_eins_20210127_webm'
hex_fc = r'C:\gis\Projects\Maps2020\Maps2020.gdb\StateHex_5sqmi_planar'

within_hex_fc = r'A:\telework_survey\wfh.gdb\operator_eins_20210127_5mihex_test_webm'
within_table = r'A:\telework_survey\wfh.gdb\operator_eins_20210127_5mihex_test_webm_table'

#: Works in the built-in interpreter, but not in conda??? WTF???
print('Summarizing...')
#: First, create a layer and dq to remove null geometries
query = "Status = 'M'"
arcpy.management.MakeFeatureLayer(operator_fc, 'operator_layer', query)
print(arcpy.management.GetCount('operator_layer'))
print(arcpy.management.GetCount(hex_fc))
#: Run Summarize Within first to drastically reduce the number of hexes to loop over
arcpy.analysis.SummarizeWithin(
    hex_fc,
    'operator_layer',
    within_hex_fc,
    keep_all_polygons='ONLY_INTERSECTING',
    group_field='USER_DEPT_NAME',
    out_group_table=within_table
)

#: Loop through all values in DEPT_NAME, creating a field for each and then adding counts from out_grouped_table to these fields, then create a custom popup with Arcade and a host of filters from that?

print('Joining...')
#: Get our table into a dataframe we can play with
grouped_table_dict = {}
with arcpy.da.SearchCursor(within_table, '*') as table_cursor:
    grouped_table_dict = {row[0]: row[1:] for row in table_cursor}

groups_df = pd.DataFrame.from_dict(grouped_table_dict, orient='index', columns=['JoinID', 'Dept', 'Count'])
all_departments = groups_df['Dept'].unique()

#: pivot so each row is now a unqiue join id and the columns are the counts of each department's count value
joins = pd.pivot(data=groups_df, values='Count', index='JoinID', columns=['Dept'])
joins.fillna(0, inplace=True)

#: dump our dataframe to a dict we can use in an insert cursor
#: {joinid: {dept1:count, dept2:count}}
joins_dict = joins.to_dict('index')

#: prep output feature class
new_fields = [[name, 'LONG'] for name in all_departments]
arcpy.management.AddFields(within_hex_fc, new_fields)

print('Writing output data...')
#: Write our new data to the output feature class
insert_fields = ['Join_ID']
insert_fields.extend([name.replace(' ', '_') for name in all_departments])
with arcpy.da.UpdateCursor(within_hex_fc, insert_fields) as updater:
    for row in updater:
        join_id = row[0]
        new_list = [join_id]
        for department in all_departments:
            new_list.append(joins_dict[join_id][department])
        row = new_list
        updater.updateRow(row)

# total = arcpy.management.GetCount(hex_fc)
# current = 0

# arcpy.management.MakeFeatureLayer(operator_fc, 'operator_layer')
# arcpy.management.MakeFeatureLayer(hex_fc, 'hex_layer')

# print(f'{datetime.datetime.now()-start}: Starting iteration')

# with arcpy.da.SearchCursor(hex_fc, 'OID@') as sc:
#     iter_start = datetime.datetime.now()
#     for row in sc:
#         current += 1
#         # print(f'{current} of {total}')
#         select = 'OBJECTID = {}'.format(row[0])
#         arcpy.management.SelectLayerByAttribute('hex_layer', 'NEW_SELECTION', select)
#         arcpy.management.SelectLayerByLocation('operator_layer', 'INTERSECT', 'hex_layer')
#         point_count = arcpy.management.GetCount('operator_layer')
#         # print(point_count)
#         if point_count:
#             iter_end = datetime.datetime.now()
#             point_times.append(iter_end - iter_start)
#         else:
#             iter_end = datetime.datetime.now()
#             no_point_times.append(iter_end - iter_start)

# end = datetime.datetime.now()

# point_average_time = sum(point_times, datetime.timedelta(0)) / len(point_times)
# no_point_average_time = sum(no_point_times, datetime.timedelta(0)) / len(no_point_times)

# print(f'Total time: {end-start}')
# print(f'Average time with points: {point_average_time}')
# print(f'Average time without points: {no_point_average_time}')
