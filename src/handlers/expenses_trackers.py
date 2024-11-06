import httpx
import os
from cloudevents.http.event import CloudEvent
from pprint import pprint
from input_file import InputFile
from response_file import ResponseFile


PARSER_URL = os.getenv(
    "BANK_STATEMENT_PARSER_URL"
)


def parse_bill(bank: str, input_file: InputFile) -> ResponseFile:
    params = {"output_format": "parquet"}

    if bank == "inter":
        params['file_password'] = os.getenv("INTER_PASSWORD")

    res = httpx.post(
        f"{PARSER_URL}/{bank}/bills",
        params=params,
        files={
            "upload_file": (input_file.name, input_file.content, input_file.contentType)
        },
        timeout=10.0
    )

    res.raise_for_status()

    response_file = ResponseFile(res.headers, res.content)

    return response_file


def parse_statement(bank: str, input_file: InputFile) -> ResponseFile:
    res = httpx.post(
        f"{PARSER_URL}/{bank}/statements",
        params={"output_format": "parquet"},
        files={
            "upload_file": (input_file.name, input_file.content, input_file.contentType)
        },
        timeout=10.0
    )

    res.raise_for_status()

    response_file = ResponseFile(res.headers, res.content)

    return response_file
