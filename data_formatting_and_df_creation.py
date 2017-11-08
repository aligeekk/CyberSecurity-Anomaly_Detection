import pandas as pd
import json
import os
import datetime
import re
import pickle


# this function takes in the ms value from the date string and formats it
def date_format(matched):
    return (matched.group()[0:3] + '.' + matched.group().replace(".", "")[3:-1] + '0' * (11 - len(matched.group())))[0:10]


def open_dataframe_pickle(name_of_pickle):
    with open(name_of_pickle, 'rb') as f:
        df_from_pickle = pickle.load(f)
    return df_from_pickle


def save_dataframe_as_pickle(frame_to_save, save_name):
    with open(save_name, 'wb') as f:
        pickle.dump(frame_to_save, f)
    return

save_path = './data_DFd_V3'
# # import raw data
# path_root = './generated_data'
# # go through each node
# nodes = os.listdir(path_root)
# for node in nodes:
#     path_node = os.path.join(path_root, node)
#     # got through each file from each node
#     files = os.listdir(path_node)
#     for file in files:
#         path_file = os.path.join(path_node, file)
#         data_list = []
#         all_key_tuples = set()
#         all_keys = set()
#         count = 0
#         with open(path_file) as f:
#             test = re.compile(':\d\d([.]|[^:])?\d*Z')
#             data_type_tuple = []
#             for line in f:
#                 count += 1
#                 print(count)
#                 temp_dict = json.loads(line)
#                 # this is for having a quick reference to the various keys and datatypes across the data space
#                 for x in temp_dict.keys():
#                     all_key_tuples.add((x, type(temp_dict[x])))
#                     all_keys.add(x)
#                 # standardize date format for datetime object
#                 if 'normalDate' in temp_dict:
#                     temp_dict['normalDate'] = test.sub(date_format, temp_dict['normalDate'])
#                     temp_datetime = datetime.datetime.strptime(temp_dict['normalDate'], "%Y-%m-%dT%H:%M:%S.%f")
#                     temp_insert = 0
#                 # incorporate datetime data into simple datatypes for mongodb and the node and file of origin
#                 temp_dict = {**temp_dict, **{'year': temp_datetime.year, 'month': temp_datetime.month,
#                                              'day': temp_datetime.day, 'hour': temp_datetime.hour,
#                                              'minute': temp_datetime.minute, 'second': temp_datetime.second,
#                                              'msec': temp_datetime.microsecond, 'node': node, 'file': file}}
#                 data_list.append(temp_dict)
#         # add remaining keys and sort for readability
#         for x in temp_dict.keys():
#             all_key_tuples.add((x, type(temp_dict[x])))
#             all_keys.add(x)
#         all_keys = list(all_keys)
#         all_keys.sort()
#         all_key_tuples = list(all_key_tuples)
#         all_key_tuples.sort(key=lambda tup: tup[0])
#
#         # convert to dataframe
#         df = pd.DataFrame(data_list)
#         # because the dataset is not very large and I'm playing with the data and data pipeline, I'm saving the
#         # imported data as a pickle prior to exporting into the mongodb database.  This is an unnecessary step that
#         # I would not do if the data was streaming or of unmanageable size.
#         save_dataframe_as_pickle([df, all_key_tuples, all_keys], node + '_' + file + '_all_data_as_df.pickle')

# import raw data
path_root = './generated_data'
# go through each node
nodes = os.listdir(path_root)
for node in nodes:
    path_node = os.path.join(path_root, node)
    # got through each file from each node
    files = os.listdir(path_node)
    for file in files:
        path_file = os.path.join(path_node, file)
        data_list = []
        count = 0
        with open(path_file) as f:
            test = re.compile(':\d\d([.]|[^:])?\d*Z')
            data_type_tuple = []
            for line in f:
                count += 1
                print(count)
                temp_dict = json.loads(line)
                # standardize date format for datetime object
                if 'normalDate' in temp_dict:
                    temp_dict['normalDate'] = test.sub(date_format, temp_dict['normalDate'])
                    temp_dict['normalDate'] = str(datetime.datetime.strptime(temp_dict['normalDate'],
                                                                         "%Y-%m-%dT%H:%M:%S.%f"))
                else:
                    temp_dict['normalDate'] = np.isnan()
                # incorporate datetime data into simple datatypes for mongodb and the node and file of origin
                temp_dict = {**temp_dict, **{'node': node, 'file': file}}
                data_list.append(temp_dict)
        # convert to dataframe
        df = pd.DataFrame(data_list)
        # because the dataset is not very large and I'm playing with the data and data pipeline, I'm saving the
        # imported data as a pickle prior to exporting into the mongodb database.  This is an unnecessary step that
        # I would not do if the data was streaming or of unmanageable size.
        save_dataframe_as_pickle(df, save_path+ '/'+ node + '_' + file + '_all_data_as_df_v3.pickle')
temp_inset = 0

# # load data and concat into a single dataframe
# path_root = './data_DFd_V2'
# files = os.listdir(path_root)
# print('importing file: ', files[0],' | ', 1, ' of ', len(files))
# df_all = open_dataframe_pickle(os.path.join(path_root, files[0]))
# for i in range(1, len(files)):
#     print('importing file: ', files[i],' | ', i+1, ' of ', len(files))
#     temp_df = open_dataframe_pickle(os.path.join(path_root, files[i]))
#     df_all = pd.concat([df_all, temp_df], axis=0, join='inner', join_axes=None, ignore_index=False,
#                        verify_integrity=False)
# # find duplicates
# duplicates = df_all[df_all.duplicated(keep=False)]
# # remove duplicates from dataset
# df_all.drop_duplicates(keep='first', inplace=True)
# df_all.sort_values('normalDate', inplace=True)
# temp_inset=0