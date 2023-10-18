import re
import requests
import json
import logging
from . import utils
from .exceptions import GreinLoaderException

LOGGER = logging.getLogger(__name__)
MAX_GREIN_DATASETS = 1000000

def load_overview(no_datasets = None) -> list:
    """ loads overview of the number of datasets given as parameter
        :param: no_samples: int, default parameter are all datasets on grein
        :type: no_datasets: int
        :return: list of dict, each dict is one dataset in GREIN
                 containing GEO id, number of samples, Species, title and summary
        :rtype: list_overview:list of dictionaries
    """
    if no_datasets == None:
        LOGGER.debug("Requesting all Datasets from GREIN")
        no_datasets = MAX_GREIN_DATASETS

    payloads = utils.GreinLoaderUtils()
    n = utils.GreinLoaderUtils.get_random_url_string_parameter()
    # create the unique random string used later for nonce parameter in url
    random_str = utils.GreinLoaderUtils.get_random_nonce_parameter()

    # base url
    grein_url = "http://www.ilincs.org/apps/grein/"

    # xhr_streaming_url will always be used for streaming requests in the code
    xhr_streaming_url = f"http://www.ilincs.org/apps/grein/__sockjs__/n={n}/xhr_streaming"

    # xhr_send_url will always be used for streaming requests in the code
    xhr_send_url = f"http://www.ilincs.org/apps/grein/__sockjs__/n={n}/xhr_send"


    LOGGER.debug("Requesting Session")
    s = requests.session()
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
        LOGGER.error("Streaming error: ", err)
        raise GreinLoaderException("Streaming error: ", err)

    for line in lines:  # decodes content of provided by the streaming request
        line_content = line.decode()
        if ("hhhhhhhhhh" in line_content):  # streaming content for the request is done and the connection initialized
            break
    try:
        xhr_send_r = s.post(xhr_send_url, data='["0#0|o|"]')
        xhr_send_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error: ", err)
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

    # requesting streaming parameter for overview page on GREIN
    try:
        xhr_send_r = s.post(xhr_send_url, data=payloads.overview_streaming())
        xhr_send_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", err)
        raise GreinLoaderException("Streaming error: ", err)

    try:
        xhr_send_r = s.post(xhr_send_url, data=payloads.overview_streaming_updata())
        xhr_send_r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Streaming error with: ", err)
        raise GreinLoaderException("Streaming error: ", err)

    url_overview = f"http://www.ilincs.org/apps/grein/session/{session_id}/dataobj/datatable?w=&nonce={random_str}"
    # requesting overview of dataset with number of datasets defined in the data parameter, given in as method parameter

    try:
        overview_stream = s.post(url_overview, data=payloads.overview_form_data(no_datasets))
        overview_stream.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Overview streaming error", err)
        raise GreinLoaderException("Overview streaming error", err)

    # process dataset
    content_ = json.loads(overview_stream.content.decode())
    content_data = content_['data']  # containing relevant data in streaming response

    # creating datastructure for return, list of dictionaries, each dict is a dataset on GREIN
    overview = {}
    list_overview = []
    for item in content_data:  # iterates list of processed return content
        d = {
            "geo_accession": _format_geo_accession(item[0]),
            "no_samples": item[1],
            "species": item[2],
            "title": item[3],
            "study_summary": item[4]
        }
        list_overview.append(d)

    return list_overview


def _format_geo_accession(geo_accession_id):
    """ helper function for formating the Geo accession id
        :param: geo_accession_id: geo accession of GRIEN Dataset
        :return: formatted geo accession for a GREIN Dataset
    """
    s = re.search("GSE[0-9]{3,}", geo_accession_id)  # searches for geo accession with at least 3 digits
    if s is not None:
        return s.group()
    else:
        return ""