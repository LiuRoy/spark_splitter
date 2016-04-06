# -*- coding=utf8 -*-
from __future__ import division
from math import log

from pyspark import SparkContext, SparkConf
from utils import str_decode, extract_hanzi, cut_sentence


def init_spark_context():
    """初始化spark环境"""
    conf = SparkConf().setAppName("spark-splitter")
    sc = SparkContext(conf=conf, pyFiles=['utils.py'])
    return sc


class PhraseInfo(object):
    """词频信息"""
    def __init__(self, frequency):
        self.frequency = frequency
        self.left_trim = {}
        self.right_trim = {}

    @staticmethod
    def calc_entropy(trim):
        """计算熵"""
        if not trim:
            return float('-inf')

        trim_sum = sum(trim.values())
        entropy = 0.0
        for _, value in trim.iteritems():
            p = value / trim_sum
            entropy -= p * log(p)
        return entropy

    def calc_is_keep_right(self):
        right_entropy = self.calc_entropy(self.right_trim)
        if right_entropy < 1.0:
            return False
        return True

    def calc_is_keep_left(self):
        left_entropy = self.calc_entropy(self.left_trim)
        if left_entropy < 1.0:
            return False
        return True

    def calc_is_keep(self):
        if self.calc_is_keep_left() and self.calc_is_keep_right():
            return True
        return False


class SplitterEngine(object):
    """分词引擎"""
    def __init__(self, spark_context, corpus):
        self.sc = spark_context
        self.corpus_path = corpus

        self.result_phrase_set = None
        self.phrase_dict_map = None
        self.final_result = {}

    def split(self):
        """spark处理"""
        raw_rdd = self.sc.textFile(self.corpus_path)

        utf_rdd = raw_rdd.map(lambda line: str_decode(line))
        hanzi_rdd = utf_rdd.flatMap(lambda line: extract_hanzi(line))

        raw_phrase_rdd = hanzi_rdd.flatMap(lambda sentence: cut_sentence(sentence))

        phrase_rdd = raw_phrase_rdd.reduceByKey(lambda x, y: x + y)
        phrase_dict_map = dict(phrase_rdd.collect())
        total_count = 0
        for _, freq in phrase_dict_map.iteritems():
            total_count += freq

        def _filter(pair):
            phrase, frequency = pair
            max_ff = 0
            for i in xrange(1, len(phrase)):
                left = phrase[:i]
                right = phrase[i:]
                left_f = phrase_dict_map.get(left, 0)
                right_f = phrase_dict_map.get(right, 0)
                max_ff = max(left_f * right_f, max_ff)
            return total_count * frequency / max_ff > 100.0

        target_phrase_rdd = phrase_rdd.filter(lambda x: len(x[0]) >= 2 and x[1] >= 3)
        result_phrase_rdd = target_phrase_rdd.filter(lambda x: _filter(x))
        self.result_phrase_set = set(result_phrase_rdd.keys().collect())
        self.phrase_dict_map = {key: PhraseInfo(val) for key, val in phrase_dict_map.iteritems()}

    def post_process(self):
        """根据熵过滤"""
        for phrase, phrase_info in self.phrase_dict_map.iteritems():
            if len(phrase) < 3:
                continue
            freq = phrase_info.frequency

            left_trim = phrase[:1]
            right_part = phrase[1:]
            if right_part in self.result_phrase_set\
                    and right_part in self.phrase_dict_map:
                p_info = self.phrase_dict_map[right_part]
                p_info.left_trim[left_trim] = p_info.left_trim.get(left_trim, 0) + freq

            right_trim = phrase[-1:]
            left_part = phrase[:-1]
            if left_part in self.result_phrase_set \
                    and left_part in self.phrase_dict_map:
                p_info = self.phrase_dict_map[left_part]
                p_info.right_trim[right_trim] = p_info.right_trim.get(right_trim, 0) + freq

        for words in self.result_phrase_set:
            if words not in self.phrase_dict_map:
                continue

            words_info = self.phrase_dict_map[words]
            if words_info.calc_is_keep():
                self.final_result[words] = words_info.frequency

    def save(self, out):
        """输出结果"""
        with open(out, 'w') as f:
            for phrase, frequency in self.final_result.iteritems():
                f.write('{}\t{}\n'.format(phrase.encode('utf8'), frequency))


if __name__ == '__main__':
    spark_content = init_spark_context()
    corpus_path = '../test/moyan.txt'
    out_path = '../test/out.txt'
    engine = SplitterEngine(spark_content, corpus_path)

    engine.split()
    engine.post_process()
    engine.save(out_path)

