import boto3
import os
import json
import pickle

def save_data(filename, data):
    #Storing data with labels
    a_file = open(filename, "wb")
    pickle.dump(data, a_file)
    a_file.close()


def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')


bucket_name = "knowledgegraphs-representationandreasoning-publicdataset-new"
s3 = boto3.client('s3')


######### Enter year and type of data
## True if data is for pre-training. Else, fine tuning data (True or False)
pretraining = True

## Enter query year (2009 - 2014 for pre-training, 2016 - 2017 for fine-tuning)
query_year = 'QTR1'
######################


prefix='BERTPretrain_10KReports_cleaned/2021/'


for obj in get_all_s3_objects(boto3.client('s3'), Bucket=bucket_name, Prefix=prefix):
    file_path = obj.get('Key')

    if not file_path.endswith('.pkl'):
        continue

    qtr = file_path.split('/')[2].split('-')[0].strip()
    folder =  '/'.join(file_path.split('/')[:3]) + '/'
    os.makedirs(folder, exist_ok=True)

    #print(qtr)
    print(file_path)


    ## Read a file from s3-bucket
    data = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = pickle.loads(data['Body'].read())

    save_data(file_path, data)

