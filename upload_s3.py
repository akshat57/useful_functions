import boto3
import pickle


def save_to_s3(s3, bucket_name, output_dir, saving_counter, input_data):
    s3_file_path =  output_dir #address of s3 folder

    # # Serialize the python-list object.
    serialized_Object = pickle.dumps(input_data)

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_file_path,
        Body=serialized_Object
        )


