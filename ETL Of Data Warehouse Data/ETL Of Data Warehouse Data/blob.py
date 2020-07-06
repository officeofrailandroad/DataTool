import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
    print("Azure Blob storage v12 - Python quickstart sample")
    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
    # created after the application is launched in a console or with Visual Studio,
    # the shell or application needs to be closed and reloaded to take the
    # environment variable into account.
    connect_str = os.getenv('EJA2t7+LfJy/JmMZAHJH3rQIoRMGUXb35kormu9X04hUZ0LRS4+wicNmNobGOHMsOqbHpd8c+vhg17l/9VCTXw==')







except Exception as ex:
    print('Exception:')
    print(ex)
