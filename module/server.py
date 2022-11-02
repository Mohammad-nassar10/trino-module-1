import http.server
from http import HTTPStatus
import socketserver
import boto3
import yaml
from fybrik_python_logging import logger, init_logger
from .s3 import get_s3_credentials_from_vault
from .trino_op import exec_query_trino


data_dict = {}


def get_details_from_conf(config_path):
    """ Parse the configuration and get the data details and policies """
    # with open(config_path, 'r') as stream:
    with open("sample-conf.yaml", 'r') as stream:
        content = yaml.safe_load(stream)
        for key, val in content.items():
            if "data" in key:
                for data in val:
                    dataset_id = data["name"]
                    name = dataset_id.split("/")[1]
                    endpoint_url = data["connection"]["s3"]["endpoint_url"]
                    transformations = base64.b64decode(data["transformations"])
                    transformations_json = json.loads(transformations.decode('utf-8'))
                    transformation = transformations_json[0]['name']
                    transformation_cols = transformations_json[0][transformation]["columns"]
                    vault_credentials = data["connection"]["s3"]["vault_credentials"]
                    creds = get_s3_credentials_from_vault(vault_credentials, dataset_id)
                    data_dict[name] = {'format': data["format"], 'endpoint_url': endpoint_url, 'path': data["path"], 'transformation': transformation,
                     'transformation_cols': transformation_cols, 'creds': creds}
    print(data_dict[name])
    return data_dict[name]

def read_file(config_path, asset_name):
    # Set log level
    init_logger("TRACE", "app", 'read-module')
    # Get the dataset details from configuration
    parse_conf_dict = get_details_from_conf(config_path)
    logger.info(asset_name)
    if asset_name not in parse_conf_dict:
        return False
    parse_conf = parse_conf_dict[asset_name]
    endpoint = parse_conf['endpoint_url']
    bucket_name = parse_conf['bucket']
    objet_key = parse_conf['object_key']
    creds = parse_conf['creds']
    aws_access_key_id = creds[0]
    aws_secret_access_key = creds[1]
    
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=None,
        region_name="",
        botocore_session=None,
        profile_name=None)

    HTTP = 'http://'
    s3client = session.client(
    's3', endpoint_url=f"{HTTP}{endpoint}")
    s3resource = session.resource(
    's3', endpoint_url=f"{HTTP}{endpoint}")
    response = s3client.list_buckets()
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')
    with open('/tmp/obj_file', 'wb') as f:
        s3client.download_fileobj(bucket_name, objet_key, f)
    return True


class HttpReadHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        self.config_path = server.config_path
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    '''
    do_GET() gets the asset name from the URL.
    for instance, if the URL is localhost:8080/userdata
    then the asset name is userdata.
    Obtain the dataset associated with the asset name, and
    return it to client.
    '''
    def do_GET(self):
        sql_query = self.path.lstrip('/')
        print("query")
        print(sql_query)
        res = exec_query_trino("admin", sql_query)

        # asset_name = self.path.lstrip('/')
        # read_success = read_file(self.config_path, asset_name)
        # if read_success == False:
        #     logger.error('asset not found or malformed configuration')
        #     self.send_response(HTTPStatus.NOT_FOUND)
        #     self.end_headers()
        #     return
      
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        self.wfile.write("response\n".encode())
        # f = open('/tmp/obj_file', 'rb')
        # while True:
        #     chunk = f.read(1024)
        #     if not chunk:
        #         break
        #     self.wfile.write(chunk)


class HttpReadServer(socketserver.TCPServer):
    def __init__(self, server_address, RequestHandlerClass,
                 config_path):
        self.config_path = config_path
        socketserver.TCPServer.__init__(self, server_address,
                                        RequestHandlerClass)


class ReadServer():
    def __init__(self, config_path: str, port: int, loglevel: str):
        server = HttpReadServer(("0.0.0.0", port), HttpReadHandler, config_path)
        server.serve_forever()