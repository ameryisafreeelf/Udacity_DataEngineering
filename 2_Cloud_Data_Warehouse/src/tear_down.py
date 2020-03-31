import configparser
import boto3

def tear_down(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME):
    """ Deletes the cluster """
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    
def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Define authentication tokens
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2",
                      )
    
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            ) 
 
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

    tear_down(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME)
    print("Deleting cluster")
    
if __name__ == "__main__":
    main()