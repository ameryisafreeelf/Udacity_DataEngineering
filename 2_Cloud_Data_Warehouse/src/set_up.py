import configparser
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import json


def setup_role(DWH_IAM_ROLE_NAME, iam):
    """ Creates a new IAM role if it isn't created already """
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    # If role exists already, we will see "Error". This is fine.
    except Exception as e:
        print(e)
        

def setup_policy(DWH_IAM_ROLE_NAME, iam):
    """ Attaches role to IAM """
    print("1.2 Attaching Policy")
    
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
        

        
def get_role(DWH_IAM_ROLE_NAME, iam):
    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    print(roleArn)
    return roleArn


def create_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    """ Creates Redshift cluster """
    try:
        response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
            
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
    # If cluster exists already, we will see "Error". This is fine.
    except Exception as e:
        print(e) 
       
        
def setup(KEY, SECRET, DWH_IAM_ROLE_NAME, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    """ Create our AWS resources: Redshift cluster, and IAM for access """
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                           region_name="us-west-2",
                      )

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           ) 
    
    setup_role(DWH_IAM_ROLE_NAME, iam)
    setup_policy(DWH_IAM_ROLE_NAME, iam)
    roleArn = get_role(DWH_IAM_ROLE_NAME, iam)

    create_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Define authentication tokens
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')
    
    # Some parameters for cloud resources
    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DB_PORT")

    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    
    setup(KEY, SECRET, DWH_IAM_ROLE_NAME, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    
if __name__ == "__main__":
    main()