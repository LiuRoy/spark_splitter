# -*- coding=utf8 -*-

import re
hanzi_re = re.compile(u"[\u4E00-\u9FD5]+", re.U)
PHRASE_MAX_LENGTH = 6


def str_decode(sentence):
    """转码"""
    if not isinstance(sentence, unicode):
        try:
            sentence = sentence.decode('utf-8')
        except UnicodeDecodeError:
            sentence = sentence.decode('gbk', 'ignore')
    return sentence


def extract_hanzi(sentence):
    """提取汉字"""
    return hanzi_re.findall(sentence)


def cut_sentence(sentence):
    """把句子按照前后关系切分"""
    result = {}
    sentence_length = len(sentence)
    for i in xrange(sentence_length):
        for j in xrange(1, min(sentence_length - i, PHRASE_MAX_LENGTH + 1)):
            tmp = sentence[i: j + i]
            result[tmp] = result.get(tmp, 0) + 1
    return result.items()
