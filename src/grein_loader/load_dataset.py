# the load_dataset(gse_id) loads the description, metadata and raw count matrix from GREIN based on the GSE id
# return values:
# description: dict
# metadata: dict
# count matrix: pandas dataframe

import io
import re
import requests
import random
import string
import logging
import json
import pandas
from typing import Tuple
from .exceptions import GreinLoaderException
from . import utils

LOGGER = logging.getLogger(__name__)


def load_dataset(gse_id: str, download_type: str="RAW") -> Tuple[dict, dict, pandas.DataFrame]:
    """ Loads a dataset from GREIN.
        :param: gse_id: The dataset's GSE id, download_type: The type of data to download for expression value, either RAW or NORMALIZED
        :type: gse_id: str
        :return: description, metadata, count_matrix of the GREIN dataset
        :rtype: description:dict, metadata:dictionary, count_matrix:pandas dataframe
    """
    if download_type != "RAW" and download_type != "NORMALIZED":
        LOGGER.error("Invalid download_type passed. Value must either by 'RAW' or 'NORMALIZED'.")
        raise ValueError("Invalid download_type passed. Value must either by 'RAW' or 'NORMALIZED'.")

    payloads = utils.GreinLoaderUtils(gse_id)
    # create the unique random string used later for nonce parameter in url
    n = utils.GreinLoaderUtils.get_random_url_string_parameter()

    # xhr_streaming_url will always be used for streaming requests in the code
    xhr_streaming_url = f"http://www.ilincs.org/apps/grein/__sockjs__/n={n}/xhr_streaming"

    # xhr_send_url will always be used for streaming requests in the code
    xhr_send_url = f"http://www.ilincs.org/apps/grein/__sockjs__/n={n}/xhr_send"

    # base url
    grein_url = "http://www.ilincs.org/apps/grein/"

    LOGGER.debug("Requesting Session")
    s = requests.session()  # requests a session on GREIN, cookies are provided within the session
    try:
        r = s.get(grein_url)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("GREIN not available with: ", grein_url)
        raise GreinLoaderException(f"Failed to contact GREIN at {grein_url}: ", err)
    LOGGER.debug("Connected to GREIN")

    # streaming request is necessary for connection parameters
    try:
        xhr_streaming_r = s.post(xhr_streaming_url, stream=True)
        lines = xhr_streaming_r.iter_lines()  # saves information provided by the streaming response
        xhr_streaming_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:  # decodes content of provided by the streaming request
        line_content = line.decode()
        if ("hhhhhhhhhh" in line_content):  # streaming content for the request is done and the connection initialized
            LOGGER.debug("Connection initialized")
            break

    # streaming request for configs and sessionId
    try:
        xhr_send_r = s.post(xhr_send_url, data='["0#0|o|"]')
        xhr_send_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    config = None
    ACK_flag = False
    for line in lines:
        line_content = line.decode()
        if line_content.startswith('a["0#0|m|'):  # search for config parameter to parse sessionId
            config_string = line_content[9:len(
                line_content) - 2].replace('\\"', '"')
            config = json.loads(config_string)
        if "ACK" in line_content:
            ACK_flag = True
            break

    if not ACK_flag:
        LOGGER.error("Streaming Error")
        raise GreinLoaderException("Streaming Error no ACK")

    session_id = config["config"]["sessionId"]

    # streaming parameters of GREIN ui init for data set with gse_id
    try:
        xhr_send_summary_r = s.post(xhr_send_url,
                                    data=payloads.ui_init_parameter())  # data needs the gse_id in the payload str
        xhr_send_summary_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:
        line_content = line.decode()  # decode content from GREIN

    LOGGER.debug("Opening new connection")
    try:
        xhr_streaming_r = s.post(xhr_streaming_url, stream=True)
        lines = xhr_streaming_r.iter_lines()
        xhr_streaming_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    try:
        LOGGER.debug("Streaming parameter")
        xhr_send_r = s.post(xhr_send_url,
                            data=payloads.method_update_parameter())  # sets initial parameters for the dataset
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:
        line_content = line.decode()
        if "ACK" in line_content:
            break

    try:
        LOGGER.debug("Streaming client parameter")
        xhr_send_r = s.post(xhr_send_url, data=payloads.client_parameter())  # sets client parameter for dataset
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", str(err))
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    try:
        LOGGER.debug("Streaming dataset")
        s.post(xhr_send_url, payloads.stream_dataset_parameter())  # sets parameter for streaming
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error")
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:
        line_content = line.decode()
        if "ACK " in line_content:
            LOGGER.debug("Data received from GREIN ")
            break

    # random string must be created for the nonce parameter in the following requests
    random_str = utils.GreinLoaderUtils.get_random_nonce_parameter()
    # the description is requested for the dataset,     
    try:
        LOGGER.debug("Request Dataset")
        description_r = s.post(
            f"http://www.ilincs.org/apps/grein/session/{session_id}/dataobj/geo_summary?w=&nonce={random_str}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "http://www.ilincs.org",
                "Referer": "http://www.ilincs.org/apps/grein/?gse=" + gse_id
            },
            data=payloads.description_formdata(100))  # Bruh wtf moment ??? 
        description_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error(f"Dataset description for {gse_id} not received")
        LOGGER.exception(err)
        raise GreinLoaderException(f"Dataset description for {gse_id} not received: ", err)

    # request necessary for the metadata labels, provided in the ui via streaming
    try:
        metadata_labels_r = s.post(xhr_send_url, data=payloads.metadata_labels_parameter())
    except requests.exceptions.HTTPError as err:
        LOGGER.error(f"Metadata labels for {gse_id} not received.")
        LOGGER.exception(err)
        raise GreinLoaderException(f"Metadata labels for {gse_id} not received: ", err)

    ui_content = []
    for line in lines:  # the streaming request provides elements elements from the ui
        line_content = line.decode()
        ui_content.append(line_content)
        if "ACK" in line_content:
            break

    # parsing the provided data from streaming for keys in the metadata, later used for the metadata dictionary
    meta_data_labels = _parse_metadata(ui_content)
    data_samples = json.loads(description_r.content.decode())
    data_content = data_samples["data"]
    sample_content = data_content[1]
    no_of_samples = sample_content[1]
    metadata_formdata = _generate_metadata_formdata(len(meta_data_labels), no_of_samples)

    # random string created for requesting the metadata
    random_str = ''.join(random.choice(string.ascii_letters) for _ in range(10))
    # metadata request, without the keys for later
    try:
        metadata_r = s.post(
            f"http://www.ilincs.org/apps/grein/session/{session_id}/dataobj/metadata_full?w=&nonce={random_str}]",
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "http://www.ilincs.org",
                "Referer": "http://www.ilincs.org/apps/grein/?gse=" + gse_id
            }, data=metadata_formdata)
    except requests.exceptions.HTTPError as err:
        LOGGER.error(f"Metadata for {gse_id} not received.")
        LOGGER.exception(err)
        raise GreinLoaderException(f"Metadata for {gse_id} not received: ", err)

    # method update for count_matrix
    try:
        xhr_send_r = s.post(xhr_send_url, data=payloads.count_matrix_parameter())
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error")
        LOGGER.exception(err)
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:
        line_content = line.decode()
        if "ACK" in line_content:
            break
    
    # in case method parameter is set to normalized, different request is send
    if download_type == "NORMALIZED":
        try: 
            xhr_send_r = s.post(xhr_send_url, data=payloads.count_matrix_normalized())
        except requests.exceptions.HTTPError as err:
            LOGGER.error("Streaming error for normailzed count matrix", err)
            raise GreinLoaderException("Streaming error for normailzed count matrix", err)

    # requesting count matrix
    try:
        count_matrix_r = s.post(f"http://www.ilincs.org/apps/grein/session/{session_id}/download/downloadcounts?w=")
    except requests.exceptions.HTTPError as err:
        LOGGER.error(f"Count Matrix for {gse_id} not received")
        LOGGER.exception(err)
        raise GreinLoaderException(f"Count Matrix for {gse_id} not received: ", err)

    LOGGER.debug("Count matrix received")

    # Before returning description, metadata and count_matrix is formatted,
    # before formatting all content from the requests must be decoded

    # formats the description with hidden method in the package,
    # the description is formatted in a dictionary containing the Study Link, Species, Title and Summary
    description = ""
    metadata = ""
    count_matrix = ""
    if description_r.status_code != 500:
        description = _format_description(
            json.loads(description_r.content.decode()))  # streaming content must be decoded

    # formats metadata to a dictionary with labels provided by metadata_labels_r and values provided by metadata_r
    if metadata_r.status_code != 500:   
        metadata = json.loads(metadata_r.content.decode())
        metadata = _format_metadata(metadata, meta_data_labels)

    # formats the count matrix provided by count_matrix_r request to a pandas dataframe
    if count_matrix_r.status_code != 500:
        count_matrix_string = count_matrix_r.content.decode()
        count_matrix_string = count_matrix_string.split("\n")
        count_matrix = pandas.read_csv(io.StringIO('\n'.join(count_matrix_string)), sep=",")
        # rename the first column name with "gene"
        count_matrix.rename(columns={count_matrix.columns[0]: str("gene")}, inplace=True)
    return description, metadata, count_matrix


def _format_description(description):
    """
    Formats raw description input from streaming request to a dictionary.
    :param: description:
    :type: description: str
    :return: description, keys: Study link, Species, Title, Summary
    :rtype: description:dict
    """
    d = {}
    data = description["data"]
    for i in data:
        if i[0] == 'Study link':
            d["Study link"] = re.search(
                "https?://(www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}([-a-zA-Z0-9()@:%_+.~#?&/=]*)",
                i[1]).group()
        if i[0] == 'Species':
            d["Species"] = "" if i[1] == 'character(0)' else i[1]
        if i[0] == 'Title':
            d["Title"] = "" if i[1] == 'character(0)' else i[1]
        if i[0] == 'Summary':
            d["Summary"] = "" if i[1] == 'character(0)' else i[1]
    return d


def _format_metadata(metadata, metadata_labels):
    """
    Formats raw metadata and metadata labels provided by streaming to create metadata dictionary
    :param: metadata, metadata_labels
    :type: metadata: str, metadata_labels: list
    :return: metadata
    :rtype: metadata:dict
    """
    mdict = {}
    data = metadata["data"]
    for item in data:
        item_dict = dict(zip(metadata_labels, item))
        mdict[item[1]] = item_dict
    return mdict


def _parse_metadata(stream_list):
    """
    parses raw metadata provided by streaming,
    :param: stream_list
    :type: stream_list: list
    :return: item_list
    :rtype: item_list: list
    """
    item_string = ""
    for i in stream_list:
        if i.find("<table class") != -1:
            item_string = i
            break
    n = item_string.replace("\\", "")
    item_list = re.findall("<th>(.*?)</th>", n)
    return item_list


def _generate_metadata_formdata(n_columns, no_samples=100):
    """
    generates formdata for metadata
    :param: number of columns used for metadata
    :type: n_columns: int
    :return: raw form data parameter for request
    :rtype: raw_form: string
    """
    raw_utils = utils.GreinLoaderUtils("")
    raw_form = raw_utils.raw_form_start()
    n = 1
    while n < n_columns-5:
        raw_form += raw_utils.raw_form_column(n)
        n = n+1
    raw_form += raw_utils.raw_form_end(no_samples)
    return raw_form
