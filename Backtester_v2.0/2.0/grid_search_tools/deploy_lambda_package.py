from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os
import boto3
from shutil import make_archive, copy2, rmtree
from distutils.dir_util import copy_tree

def general_copy(src, dst):
    if os.path.isdir(src):
        newdst = os.path.join(dst, os.path.basename(src))
        if os.path.isdir(newdst):
            rmtree(newdst)
        os.mkdir(newdst)
        copy_tree(src, newdst)
    else:
        copy2(src, dst)

def deploy_lambda_package(deployment_package_dir, source_package_dir, zip_package_name, src_dst_relative_path_tupes):
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
    AWS_ACCESS_SECRET = os.environ.get("AWS_ACCESS_SECRET")
    REGION_NAME = os.environ.get("REGION_NAME")
    
    FUNCTION_NAME = os.environ.get("FUNCTION_NAME")
    BUCKET_NAME = os.environ.get("BUCKET_NAME")

    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_ACCESS_SECRET,
        region_name=REGION_NAME)

    #copy lambda function to package
    for sd in src_dst_relative_path_tupes:
        print('Copying {} to {}'.format(os.path.join(source_package_dir,sd[0]),
                                        os.path.join(deployment_package_dir,sd[1])))
        general_copy(os.path.join(source_package_dir,sd[0]), os.path.join(deployment_package_dir,sd[1]))

    #remove old zipped package
    if(os.path.isfile(os.path.join(source_package_dir,zip_package_name))):
        os.remove(os.path.join(source_package_dir,zip_package_name))

    #make new zipped package
    print('Generating Deployment Package (usually takes 30-60sec)...')
    make_archive(zip_package_name.split('.')[0],zip_package_name.split('.')[1],root_dir=deployment_package_dir)

    #upload zip to s3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_ACCESS_SECRET,
        region_name=REGION_NAME)

    print('Uploading to S3...')
    s3_client.upload_file(os.path.join(source_package_dir,zip_package_name), BUCKET_NAME, zip_package_name)

    print('Uploading to Lambda...')
    #upload zip from s3 to lambda
    lambda_client.update_function_code(
      FunctionName=FUNCTION_NAME,
      S3Bucket=BUCKET_NAME,
      S3Key=zip_package_name,
    )
    print('Done')