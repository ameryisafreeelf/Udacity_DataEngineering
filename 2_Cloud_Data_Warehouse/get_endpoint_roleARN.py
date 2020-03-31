import configparser
import pandas as pd
import boto3


def prettyRedshiftProps(props):
    """ Formatting for metadata about cluster """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])        
    
    
def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    """ Returns DWG_ENDPOINT and DWG_ROLE_ARN for cluster """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Define authentication tokens
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            ) 
    
    get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
if __name__ == "__main__":
    main()