from google.cloud.storage import Client

class InputFile:
    def __init__(self, bucket_client: Client, bucket_name: str, name: str, content_type: str) -> None:
        self.bucket_name = bucket_name
        self.name = name
        self.content_type = content_type
        
        self.bucket = bucket_client.get_bucket(bucket_name)
        self.blob = self.bucket.blob(name)
        self.content = self.blob.download_as_bytes()