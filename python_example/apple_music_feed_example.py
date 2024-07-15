""""
 For licensing see accompanying LICENSE file.
 Copyright (C) 2024 Apple Inc. All Rights Reserved.
"""

"""
Example for how to get the urls for the latest dump from an Apple Music Feed, and then store them
"""

import click
import jwt
import requests
from requests.adapters import HTTPAdapter, Retry
from time import time
from typing import Set, Tuple
from pyspark.sql import SparkSession
import logging
from multiprocessing.pool import ThreadPool
from pathlib import Path
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)


class InvalidTeamIdException(Exception):
    pass


class InvalidKeyIdException(Exception):
    pass


class InvalidOrMissingExportIdException(Exception):
    pass


@dataclass(frozen=True)
class PartLocation:
    """Class for keeping where a part is and where it should go"""

    outputPath: Path
    url: str


@dataclass()
class PartResult:
    """Class for keeping a list of part locations and the next link"""

    part_locations: set[PartLocation]
    next_link: str


def generate_jwt(secret_key_file_path: str, team_id: str, key_id: str) -> str:
    if len(team_id) != 10:
        raise InvalidTeamIdException
    if len(key_id) != 10:
        raise InvalidKeyIdException

    current_unix_seconds = int(time())
    with open(secret_key_file_path, "rb") as f:
        jwt_payload = {
            "exp": current_unix_seconds + (24 * 60 * 60),  # expiration time
            "iss": team_id,  # issuer
            "iat": current_unix_seconds,  # issued at
        }

        jwt_token = jwt.encode(
            jwt_payload, f.read(), algorithm="ES256", headers={"kid": key_id}
        )
        return jwt_token


def create_request_with_jwt(url: str, jwt: str) -> requests.PreparedRequest:
    return requests.Request(
        "GET",
        url,
        headers={"Authorization": "Bearer " + jwt}
    ).prepare()


def handle_request_with_jwt(s: requests.Session, url: str, jwt: str):
    prepped_request = create_request_with_jwt(url, jwt)
    response = s.send(prepped_request)
    if response.status_code != 200:
        if response.status_code == 401:
            raise Exception(
                "Authentication Failed. Did you provide the correct team id, key id, and p8 file?"
            )
        elif response.status_code == 404:
            # 404 is not always wrong for us, that might be empty
            response.raise_for_status()
        else:
            raise Exception(
                f"Request to {url} failed. Status Code = {str(response.status_code)}"
            )
    return response.json()


def get_feed_urls_by_limit_and_offset(
    s: requests.Session, url: str, jwt: str, out_dir: Path
) -> PartResult:
    try:
        parts_response = handle_request_with_jwt(s, url, jwt)
        part_locations = set(
            map(
                lambda x: PartLocation(
                    out_dir / x["id"], x["attributes"]["exportLocation"]
                ),
                parts_response["resources"]["parts"].values(),
            )
        )
        next_link = parts_response["next"] if "next" in parts_response else None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            part_locations = set()
            next_link = None
        else:
            raise e
    return PartResult(part_locations, next_link)


def get_feed_urls(
    feed_name: str,
    latest_id: str,
    apple_music_feed_base_url: str,
    s: requests.Session,
    jwt: str,
    out_dir: Path,
) -> Set[PartLocation]:
    limit = 10
    logger.info(f"The latest export for feed {feed_name} has id {latest_id}")
    logger.info(f"Getting the Apache Parquet file URLs from export {latest_id}...")
    parts_url = (
        apple_music_feed_base_url
        + "/v1/feed/exports/"
        + latest_id
        + f"/parts?limit={limit}"
    )

    part_result = get_feed_urls_by_limit_and_offset(s, parts_url, jwt, out_dir)
    part_locations = part_result.part_locations
    next_link = part_result.next_link

    logger.info(f"Received {len(part_locations)} part urls")

    while next_link:
        offset = parse_qs(urlparse(next_link).query)["offset"][0]
        logger.info(
            f"Getting next Apache Parquet urls. Offset = {offset}, limit = {limit}"
        )
        next_parts_url = apple_music_feed_base_url + next_link
        part_result = get_feed_urls_by_limit_and_offset(s, next_parts_url, jwt, out_dir)

        part_locations.update(part_result.part_locations)
        logger.info(f"Received {len(part_result.part_locations)} at offset = {offset}")
        next_link = part_result.next_link
    return part_locations


def download_file(part_location: PartLocation):
    initial_current_time = time()
    output_path = part_location.outputPath
    url = part_location.url
    response = requests.get(url, stream=True)
    with open(output_path, "wb") as fw:
        for chunk in response.iter_content(chunk_size=102400):
            fw.write(chunk)
    new_current_time = time()
    logger.info(
        f"downloaded file to {str(output_path)} in {str(new_current_time - initial_current_time)} seconds"
    )


@click.command()
@click.option(
    "--key-id", help="id of a key linked to your Apple Developer account", required=True
)
@click.option(
    "--team-id", help="id of your Apple Developer account team", required=True
)
@click.option(
    "--secret-key-file-path",
    help="file path to your secret key. Should be in a .p8 format",
    required=True,
)
@click.option(
    "--apple-music-feed-base-url",
    help="url of Apple Music feed",
    default="https://api.media.apple.com",
)
@click.option("--feed-name", help="Feed name you wish to download", default="artist")
@click.option(
    "--out-dir", help="Directory where to store the Apache Parquet files", required=True
)
def get_latest_feed_parts(
    key_id: str,
    team_id: str,
    secret_key_file_path: str,
    apple_music_feed_base_url: str,
    feed_name: str,
    out_dir: str,
):
    s = requests.Session()
    # add a geometric retry for rate limiting
    retries = Retry(
        total=5, backoff_factor=1, backoff_jitter=0.1, status_forcelist=[429]
    )  # 429 means we're rate limited, so we want to back off and try again
    s.mount("https:", HTTPAdapter(max_retries=retries))

    logger.info(f"Sending requests to {apple_music_feed_base_url}")
    jwt: str = generate_jwt(secret_key_file_path, team_id, key_id)

    logger.info(f"Getting the latest export for feed {feed_name}")
    latest_url = f"{apple_music_feed_base_url}/v1/feed/{feed_name}/latest"
    latest = handle_request_with_jwt(s, latest_url, jwt)

    latest_id = latest["data"][0]["id"]
    if not latest_id:
        raise InvalidOrMissingExportIdException

    out_dir_path = Path(out_dir)
    part_locations = get_feed_urls(
        feed_name, latest_id, apple_music_feed_base_url, s, jwt, out_dir_path
    )
    logger.info(f"Found {len(part_locations)} total")

    with ThreadPool(8) as p:
        p.map(download_file, part_locations)

    logger.info("Finished downloading files")
    read_parquet(str(out_dir))


def read_parquet(parquet_location: str):
    """Reads Apache Parquet from the location specified in 'parquet_location' and prints the name and release date for 5 sample items.
    Sample printed output:

        +----------+-----------------+
        |        id|      nameDefault|
        +----------+-----------------+
        | 318593498|Send Me Your Love|
        |1272908498|            Happy|
        |1286748698|          Beleléu|
        |1315824098|  Besos Callejero|
        |1113028898|         Ramnusia|
        +----------+-----------------+
    """

    spark = (
        SparkSession.builder.appName("Parquet Reader")
        .config("spark.master", "local")
        .getOrCreate()
    )

    # Read Parquet data and select two fields to load into a Spark DataFrame
    #
    dataframe = spark.read.parquet(parquet_location).select("id", "nameDefault")

    # Print 5 records
    #
    dataframe.show(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("running....")
    get_latest_feed_parts()
