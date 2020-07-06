import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


def main():
    export_to_blob()

def export_to_blob(source_path,source_file_name):
    try:
        
        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        container_name = 'data-warehouse-blob'

        # Define the container
        container_client = blob_service_client.get_container_client(container_name)

        # Create a file in local data directory to upload and download
        local_path = source_path
        local_file_name = source_file_name
        upload_file_path = os.path.join(local_path, local_file_name)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    except Exception as ex:
        print('Exception:')
        print(ex)


if __name__ == '__main__':
    main()