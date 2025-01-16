from yaml import full_load
import json
from collections import Counter
from bm25s.tokenization import Tokenizer
import jieba
import re
from utils.util_log import test_log as log


def custom_tokenizer(language="en"):
    def remove_punctuation(text):
        text = text.strip()
        text = text.replace("\n", " ")
        return re.sub(r'[^\w\s]', ' ', text)

    # Tokenize the corpus
    def jieba_split(text):
        text_without_punctuation = remove_punctuation(text)
        return jieba.lcut(text_without_punctuation)

    def blank_space_split(text):
        text_without_punctuation = remove_punctuation(text)
        return text_without_punctuation.split()

    stopwords = [" "]
    stemmer = None
    if language in ["zh", "cn", "chinese"]:
        splitter = jieba_split
        tokenizer = Tokenizer(
            stemmer=stemmer, splitter=splitter, stopwords=stopwords
        )
    else:
        splitter = blank_space_split
        tokenizer = Tokenizer(
            stemmer=stemmer, splitter= splitter, stopwords=stopwords
        )
    return tokenizer


def analyze_documents(texts, language="en"):

    tokenizer = custom_tokenizer(language)
    new_texts = []
    for text in texts:
        if isinstance(text, str):
            new_texts.append(text)
    # Tokenize the corpus
    tokenized = tokenizer.tokenize(new_texts, return_as="tuple")
    # log.info(f"Tokenized: {tokenized}")
    # Create a frequency counter
    freq = Counter()

    # Count the frequency of each token
    for doc_ids in tokenized.ids:
        freq.update(doc_ids)
    # Create a reverse vocabulary mapping
    id_to_word = {id: word for word, id in tokenized.vocab.items()}

    # Convert token ids back to words
    word_freq = Counter({id_to_word[token_id]: count for token_id, count in freq.items()})
    log.debug(f"word freq {word_freq.most_common(10)}")

    return word_freq


def gen_experiment_config(yaml):
    """load the yaml file of chaos experiment"""
    with open(yaml) as f:
        _config = full_load(f)
        f.close()
    return _config


def findkeys(node, kv):
    # refer to https://stackoverflow.com/questions/9807634/find-all-occurrences-of-a-key-in-nested-dictionaries-and-lists
    if isinstance(node, list):
        for i in node:
            for x in findkeys(i, kv):
                yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in findkeys(j, kv):
                yield x


def update_key_value(node, modify_k, modify_v):
    # update the value of modify_k to modify_v
    if isinstance(node, list):
        for i in node:
            update_key_value(i, modify_k, modify_v)
    elif isinstance(node, dict):
        if modify_k in node:
            node[modify_k] = modify_v
        for j in node.values():
            update_key_value(j, modify_k, modify_v)
    return node


def update_key_name(node, modify_k, modify_k_new):
    # update the name of modify_k to modify_k_new
    if isinstance(node, list):
        for i in node:
            update_key_name(i, modify_k, modify_k_new)
    elif isinstance(node, dict):
        if modify_k in node:
            value_backup = node[modify_k]
            del node[modify_k]
            node[modify_k_new] = value_backup
        for j in node.values():
            update_key_name(j, modify_k, modify_k_new)
    return node


def get_collections():
    try:
        with open("/tmp/ci_logs/all_collections.json", "r") as f:
            data = json.load(f)
            collections = data["all"]
    except Exception as e:
        log.error(f"get_all_collections error: {e}")
        return []
    return collections


if __name__ == "__main__":
    d = {
        "id": "abcde",
        "key1": "blah",
        "key2": "blah blah",
        "nestedlist": [
            {
                "id": "qwerty",
                "nestednestedlist": [
                    {"id": "xyz", "keyA": "blah blah blah"},
                    {"id": "fghi", "keyZ": "blah blah blah"},
                ],
                "anothernestednestedlist": [
                    {"id": "asdf", "keyQ": "blah blah"},
                    {"id": "yuiop", "keyW": "blah"},
                ],
            }
        ],
    }
    print(list(findkeys(d, "id")))
    update_key_value(d, "none_id", "ccc")
    print(d)
