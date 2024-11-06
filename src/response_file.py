from httpx import Headers

class ResponseFile:
    def __init__(self, headers: Headers, content: bytes) -> None:
        self.headers = headers
        self.content = content
        self.name = None

        if 'content-disposition' in headers:
            self.name = headers['content-disposition'].split('filename=')[1]