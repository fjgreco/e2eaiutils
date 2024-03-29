import ibm_boto3
from ibm_botocore.client import Config, ClientError
from datetime import datetime
import time
import json
import os


class ICOS:
    def __init__(self,icos_credentials=''):
        
        self.icos_credentials=icos_credentials
        
        # Constants for IBM COS values
        COS_ENDPOINT = "https://s3.us.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
        COS_API_KEY_ID = self.icos_credentials["apikey"] # eg "W00YiRnLW4a3fTjMB-odB-2ySfTrFBIQQWanc--P3byk"
        #COS_AUTH_ENDPOINT = 'https://iam.bluemix.net/oidc/token'
        COS_AUTH_ENDPOINT = 'https://iam.cloud.ibm.com/identity/token'
        COS_RESOURCE_CRN = self.icos_credentials["resource_instance_id"]

        # Create resource
        self.cos = ibm_boto3.resource("s3",
            ibm_api_key_id=COS_API_KEY_ID,
            ibm_service_instance_id=COS_RESOURCE_CRN,
            ibm_auth_endpoint=COS_AUTH_ENDPOINT,
            config=Config(signature_version="oauth"),
            endpoint_url=COS_ENDPOINT
        )
        self.cos_client = ibm_boto3.client("s3",
            ibm_api_key_id=COS_API_KEY_ID,
            ibm_service_instance_id=COS_RESOURCE_CRN,
            ibm_auth_endpoint=COS_AUTH_ENDPOINT,
            config=Config(signature_version="oauth"),
            endpoint_url=COS_ENDPOINT
        )

    # create_bucket
    def create_bucket(self,bucket):  
        try:
            res=self.cos.create_bucket(Bucket=bucket)
        except Exception as e:
            print(Exception, e)
        else:
            return (res)
        
    # list buckets
    import json
    def list_buckets(self):  
        try:
            res=self.cos_client.list_buckets()
        except Exception as e:
            print(Exception, e)
        else:
            return (res) 
        
        
    # list prefixed buckets
    def list_prefix_buckets(self,prefix='results-'):  
        try:
            res=self.cos_client.list_buckets()
        except Exception as e:
            print(Exception, e)
        else:
            for bucket in res['Buckets']:
                if prefix in bucket['Name']:
                    print('\nBucket:',bucket['Name'])
                    
    # list objects
    def list_objects(self,bucket_name,verbose=False):

        print("Retrieving bucket contents from: {}\n".format(bucket_name)) 
        
        object_list=[]

        try:
            files = self.cos.Bucket(bucket_name).objects.all()

            for item in files: 
                object_list.append(item.key)
                if verbose:
                    print(item.key)

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))

        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        
        return object_list

            
    # download list
    def download_list(self,bucket_name,results_folder='.'):

        print("Retrieving relevant bucket contents from: {}\n".format(bucket_name)) 
        download_list=[]
        try:
            files = self.cos.Bucket(bucket_name).objects.all()
            for file in files: 
                item=file.key.split('/')
                if 'bioinformatics' in item[1] or 'training-log.txt' in item[1]:
                    print('saving:',file.key)
                    download_list.append(file.key)
                    fn=file.key.split('/')[1]
                    self.cos_client.download_file(bucket_name,Key=file.key,Filename=results_folder+'/'+fn)

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        return download_list
   

     # upload file
    def upload_file(self,bucket,local_file_name,key):  
        try:
            res=self.cos_client.upload_file(Bucket=bucket,
                                  Key=key,
                                  Filename=local_file_name)
        except Exception as e:
            print(Exception, e)
        else:
            print('Uploaded {} to {}'.format(local_file_name,key))
            return (res)
                  
    # upload folder              
    def upload_folder(self,bucket,folder):
        print("Invoking uploadfolder")
        for filename in os.scandir(folder):
            if filename.is_file():
                self.upload_file(bucket,filename.path,filename.path)



    # download file
    def download_file(self,bucket,key,local_file_name):  
        try:
            res=self.cos_client.download_file(Bucket=bucket,
                                  Key=key,
                                  Filename=local_file_name)
        except Exception as e:
            print(bucket,key,local_file_name)
            print(Exception, e)
        else:
            print('Downloaded {} to {}'.format(key,local_file_name))
            return (res)
         
            
    # delete file
    def delete_file(self,bucket,key):  

        try:
            res=self.cos_client.delete_object(Bucket=bucket,Key=key)
        except Exception as e:
            print(Exception, e)
        else:
            print('Deleted {} from {}.'.format(key,bucket))
            
            
     # delete files
    def delete_files(self,bucket):  

        try:
            res=self.cos_client.list_objects(Bucket=bucket)
        except Exception as e:
            print(Exception, e)
        else:
            print('Deleting objects')
            count=0
            total=0
            for item in res['Contents']:
                count += 1
                print(item['Key'])
                self.cos_client.delete_object(Bucket=bucket,Key=item['Key'])
            total += count
            print ('Count: {} Total: {}'.format(count, total))
          
   # delete bucket
    def delete_bucket(self,bucket):  

        try:
            res=self.cos_client.delete_bucket(Bucket=bucket)
        except Exception as e:
            print(Exception, e)
        else:
            print('Deleted bucket:', bucket)

    # Get download list
    def get_download_list(self,bucket_name,results_folder='RESULTS'):
    
        print("Retrieving relevant bucket contents from: {}\n".format(bucket_name)) 
        download_list=[]
        try:
            files = self.cos.Bucket(bucket_name).objects.all()
            for file in files: 
                item=file.key.split('/')
                if 'bioinformatics' in item[1] or 'training-log.txt' in item[1]:
                    print('saving:',file.key)
                    download_list.append(file.key)
                    fn=file.key.split('/')[1]
                    self.cos_client.download_file(bucket_name,Key=file.key,Filename=results_folder+'/'+fn)

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        return download_list

            
    def get_download_list_loc(self,bucket_name,model_location,results_folder='RESULTS'):

        print("Retrieving relevant bucket contents from: {} Model_location: {}\n".format(bucket_name,model_location)) 
        download_list=[]
        try:
            files = self.cos.Bucket(bucket_name).objects.all()
            for file in files: 
                item=file.key.split('/')
                if ('bioinformatics' in item[1] or 'training-log.txt' in item[1]) and model_location in item[0]:
                    print(file.key)
                    download_list.append(file.key)
                    fn=file.key.split('/')[1]
                    self.cos_client.download_file(bucket_name,Key=file.key,Filename=results_folder+'/'+fn)

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        return download_list  



    def list_results_buckets(self):  
        try:
            res=self.cos_client.list_buckets()
        except Exception as e:
            print(Exception, e)
        else:
            for bucket in res['Buckets']:
                if 'results-' in bucket['Name']:
                    print('\nBucket:',bucket['Name'])


    def get_request_json(self,bucket_name,results_folder='RESULTS'):

        print("Retrieving request.json from: {}\n".format(bucket_name)) 
        download_list=[]
        try:
            files = self.cos.Bucket(bucket_name).objects.all()
            for file in files: 
                if  results_folder in file.key and 'request.json' in file.key:
                    print('saving:',file.key)
                    download_list.append(file.key)
                    self.cos_client.download_file(bucket_name,Key=file.key,Filename='request.json')

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        return download_list   
            
        
    def get_stream_data(self,bucket,key):
        streaming_body=self.cos_client.get_object(Bucket=bucket, Key=key)['Body']
        data=streaming_body.read().decode('ascii')
        return data
            
    def get_stream_body(self,bucket,key):
        streaming_body=self.cos_client.get_object(Bucket=bucket, Key=key)['Body']
        return streaming_body
    
    def get_stream(self,bucket,key):
        streaming=self.cos_client.get_object(Bucket=bucket, Key=key)
        return streaming
   
    def stage_multiple_fa(self,bucket,staging_dir,sequence_fn='sequences.txt',labels_fn='labels.txt'):
        
        count = 0
        
        fs=open(f'{staging_dir}/{sequence_fn}','w+')

        fl=open(f'{staging_dir}/{labels_fn}','w+') 

        for item in self.list_objects(bucket):
            parsed=item.split('/')
            fn=parsed[1]
            print('\nfn:',fn)
            parsed_1=fn.split('.')

            if parsed_1[1] != 'fa':
                pass

            #print('parsed_1:',parsed_1)
            parsed_2=parsed_1[0].split('_')
            #print("parsed_2",parsed_2)
            label=parsed_2[4]
            #print("label:", label)

            target=f'{staging_dir}/{fn}'

            self.download_file(bucket,item,target)


            with open(target,'r') as fd:
                content=fd.read()
                content_list=content.split(',')
                print(content_list[0][0:20],'...')

                fs.write(content_list[0]+'\n')
                fl.write(content_list[1]+'\n')

            os.remove(target)
            
            count += 1
            print("Count:",count)

        fs.close()
        fl.close()
        return True


  
