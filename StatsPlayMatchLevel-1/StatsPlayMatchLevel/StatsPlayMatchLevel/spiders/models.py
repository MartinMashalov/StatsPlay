
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from typing import Union

def find_most_similar_key(input_str, data_ref: Union[dict, list]) -> str:
    """get the best match among the keys in the dictionary """

    # check if the type is list or dictionary
    if isinstance(data_ref, dict):
        best_match = process.extractOne(input_str, data_ref.keys())
    elif isinstance(data_ref, list):
        best_match = process.extractOne(input_str, data_ref)
    else:
        raise Exception("Incorrect Type in Similarity Search: 404")
    # return the best matching key
    return best_match[0]