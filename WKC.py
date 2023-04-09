import subprocess
import json
import requests

class WKC:
    # Run Command
    def __init__(self,apikey='', catalog_name=''):
        
        self.apikey=apikey
        if self.apikey:
            self.set_bearer_token(self.apikey)
        else:
            print("Warning: apikey has not been set.")
         
        self.catalog_name=catalog_name
        self.guid=''
        if self.catalog_name:
            self.guid=self.get_catalog_guid(catalog_name,verbose=False)
            self.catalog_name=catalog_name
        else:
            print("Warning: guid has not been set.")
            
    def set_bearer_token(self,apikey):
        cmd='curl -k -X POST \
          --header "Content-Type: application/x-www-form-urlencoded" \
          --header "Accept: application/json" \
          --data-urlencode "grant_type=urn:ibm:params:oauth:grant-type:apikey" \
          --data-urlencode "apikey={}" \
          "https://iam.cloud.ibm.com/identity/token"'.format(apikey)
        results=self.run(cmd)
        self.BearerToken=results['access_token']
      
    def set_BearerToken(self,token):
        self.BearerToken=token
        
    def set_guid(self,guid):
        self.guid=guid
        metadata=self.get_catalog_metadata(self)
        self.catalog_name=metadata['entity']['name']
        
    def set_catalog_name(self,catalog_name):
        self.catalog_name=catalog_name
        self.guid=self.get_catalog_guid(catalog_name)
        
    def set_apikey(self,apikey):
        self.apikey=apikey
        
        
    def run(self,command,verbose=False):
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()

        out_dict=json.loads(out.decode())
        print(err.decode(),'\n')
        if verbose: 
            print(json.dumps(out_dict,indent=2))
        return out_dict 

    # Get catalogs
    def get_catalogs(self,verbose=False):
        command ='curl -H  "Authorization: {}" https://api.dataplatform.cloud.ibm.com/v2/catalogs'.format(self.BearerToken) 
        catalogs=self.run(command,verbose=verbose)
        return catalogs

    
    # Get guid using catalog name
    def get_catalog_guid(self,catalog_name,verbose=False):
        guid='Not Found'
        catalogs=self.get_catalogs(verbose=verbose)
        for item in catalogs['catalogs']:
            if item['entity']['name']==catalog_name:
                guid= item['metadata']['guid']
                break
        return guid
    
    # Get catalog name using guid
    def get_catalog_metadata(self,verbose=False):
        command ='curl -H  "Authorization: {}" https://api.dataplatform.cloud.ibm.com/v2/catalogs/{}'.format(self.BearerToken,self.guid) 
        catalog_metadata=self.run(command,verbose=verbose)
        return catalog_metadata

    # Get asset buckets
    def get_asset_buckets(self,verbose=True):
        command ='curl -H  "Authorization: {}" https://api.dataplatform.cloud.ibm.com/v2/catalogs/{}/asset_buckets'.format(self.BearerToken,self.guid) 
        asset_buckets=self.run(command,verbose=verbose)
        return asset_buckets

    # Get asset types
    def get_asset_types(self,verbose=True):
        command ='curl -H  "Authorization: {}" https://api.dataplatform.cloud.ibm.com/v2/asset_types?catalog_id={}'.format(self.BearerToken,self.guid) 
        asset_types=self.run(command,verbose=verbose)
        return asset_types   

    #Get asset list
    def get_asset_list(self,asset_type,verbose=False):
        headers = {
            'Content-type': 'application/json',
            'Authorization': self.BearerToken
        }

        url='https://api.dataplatform.cloud.ibm.com/v2/catalogs/{}/types/{}/search'.format(self.guid,asset_type)

        data = '{"query":"asset.asset_state:available"}'

        response = requests.post(url, headers=headers, data=data)

        response_dict=json.loads(response.text)

        if verbose:
            print(json.dumps(response_dict,indent=2))

        summary_list=[]
        for item in response_dict['results']:
            summary_list.append(item['metadata']['name'])

        return response_dict,summary_list

    #Get asset id
    def get_asset_id(self,asset_name,asset_type,verbose=False):
        asset_list_dict,_ = self.get_asset_list(asset_type)
        asset_id='Not Found'
        for item in asset_list_dict['results']:
            if item['metadata']['name']==asset_name:
                asset_id=item['metadata']['asset_id']
                break
        if verbose:
            print('asset_id for {}: {}'.format(asset_name,asset_id))
        return asset_id

  # Get asset metadata
    def get_asset_metadata(self,asset_name,asset_type,verbose=True):
        asset_id=self.get_asset_id(asset_name,asset_type,verbose=True)
        command ='curl -H  "Authorization: {}" https://api.dataplatform.cloud.ibm.com/v2/assets/{}?catalog_id={}'.format(self.BearerToken,asset_id,self.guid) 
        asset=self.run(command,verbose=verbose)
        if verbose:
            print(json.dumps(asset,indent=2))
        return asset  
 
    # Add folder_asset
    def add_asset(self,metadata):
        command ='curl -d "@{}"  -H "Content-Type: application/json" -H "Authorization: {}"  -X POST   https://api.dataplatform.cloud.ibm.com/v2/assets?catalog_id={}'.format(metadata,self.BearerToken,self.guid) 
        asset=self.run(command)
        print(json.dumps(asset,indent=2))
        
    def add_asset_inline(self,metadata):
        import os
        import json
        try:
            with open('metadata_tmp.json', 'w') as fp:
                json.dump(metadata,fp)
            self.add_asset('metadata_tmp.json')
        finally:
            os.remove('metadata_tmp.json')

        
    # Create asset type
    def create_asset_type(self,metadata):
        command ='curl -d "@{}"  -H "Content-Type: application/json" -H "Authorization: {}"  -X POST   https://api.dataplatform.cloud.ibm.com/v2/asset_types?catalog_id={}'.format(metadata,self.BearerToken,self.guid) 
        asset=self.run(command)
        print(json.dumps(asset,indent=2))
