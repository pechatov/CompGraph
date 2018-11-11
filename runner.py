from collections import Counter
from graph.graph import Graph
from math import log
from pytest import approx
import string


def _filter_punctuation(txt):
    p = set(string.punctuation)
    return "".join([c for c in txt if c not in p])

# ************************_____build_word_count_graph_____************************


def tokenizer_mapper(r, doc_column, text_column):
    """
       splits rows with 'text' field into set of rows with 'token' field
      (one for every occurence of every word in text)
    """

    tokens = _filter_punctuation(r[text_column]).lower().split()

    for token in tokens:
        yield {
            doc_column: r[doc_column],
            text_column: token,
        }


def counting_reducer(records, text_column, count_column):
    cnt = 0
    for x in records:
        cnt += 1
        word = x[text_column]

    yield {count_column: cnt, text_column: word}
# ********************************************************************************

# ************************_____build_inverted_index_graph_____********************


def count_rows(state, row):
    if state is None:
        state = {'count': 0}
    state['count'] += 1

    return state


def term_frequency_reducer(records, doc_column, text_column):
    '''
        calculates term frequency for every word in doc_id
    '''

    word_count = Counter()

    for r in records:
        word_count[r[text_column]] += 1

    total = sum(word_count.values())
    for w, count in word_count.items():
        yield {
            doc_column: r[doc_column],
            text_column: w,
            'tf': count / total
        }


def unique(records, doc_column, text_column):
    # yield {doc_column: records[doc_column], text_column: records[text_column]}
    for x in records:
        yield x
        break


def invert_index(records, doc_column, text_column):
    result = []
    for row in records:
        result.append({text_column: row[text_column], doc_column: row[doc_column], "tf_idf": row['tf'] * row['idf']})
    for x in sorted(result, key=lambda x: x['tf_idf'], reverse=True)[:3]:
        yield x


def calc_idf(records, doc_column, text_column):
    elements = set()
    word = 0
    counter = 0
    for r in records:
        elements.add(r[doc_column])
        if word == 0:
            word = r[text_column]
            counter = r['count']

    yield {
        text_column: word,
        'idf': log(counter / len(elements))
    }

# ********************************************************************************


def tfr_for_all(records, text_column):
    '''
        calculates term frequency for every word in doc_id
    '''
    count = 0
    for row in records:
        count += 1

    yield {
        text_column: row[text_column],
        'tf_for_all': count / row['count']
    }


def pmi_counter(records, doc_column, text_column):
    result = []
    for row in records:
        result.append({text_column: row[text_column], doc_column: row[
                      doc_column], 'pmi': log(row['tf'] / row['tf_for_all'])})
    for x in sorted(result, key=lambda x: x['pmi'], reverse=True)[:10]:
        yield x


def drop_less_then_two(records, doc_column, text_column):
    word_count = Counter()

    for r in records:
        word_count[r[text_column]] += 1

    for w, count in word_count.items():
        if count >= 2:
            for i in range(count):
                yield {
                    doc_column: r[doc_column],
                    text_column: w
                }


def my_parser(f):
    result = []
    for row in f:
        result.append(eval(row.strip()))
    return result


def build_inverted_index_graph(input_stream, doc_column='doc_id', text_column='text'):
    split_words = Graph()
    split_words.input('rows').map(tokenizer_mapper, doc_column=doc_column, text_column=text_column)

    count_docs = Graph()
    count_docs.input('rows').fold(folder=count_rows)

    count_idf = Graph()
    count_idf.input(split_words).sort(key=(doc_column, text_column))\
        .reduce(reducer=unique, key=(doc_column, text_column), doc_column=doc_column, text_column=text_column)\
        .join(count_docs, method='cross')\
        .sort(key=text_column)\
        .reduce(reducer=calc_idf, key=text_column, doc_column=doc_column, text_column=text_column)

    calc_index = Graph()
    calc_index.input(split_words).sort(key=doc_column)\
        .reduce(reducer=term_frequency_reducer, key=doc_column, doc_column=doc_column, text_column=text_column)\
        .join(count_idf, key=(text_column, text_column), method='left')\
        .sort(key=text_column)\
        .reduce(reducer=invert_index, key=text_column, doc_column=doc_column, text_column=text_column)

    return calc_index


def main():
    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!'}
    ]

    rows1 = [
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? little'},
        {'doc_id': 5, 'text': 'HELLO HELLO!'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!'}
    ]
    doc_column = 'doc_id'
    text_column = 'text'
    input_stream = 'rows'

    etalon = [
        {"doc_id": 3, "text": "little", "pmi": approx(0.9555, 0.001)},
        {"doc_id": 4, "text": "little", "pmi": approx(0.9555, 0.001)},
        {"doc_id": 5, "text": "hello", "pmi": approx(1.1786, 0.001)},
        {"doc_id": 6, "text": "world", "pmi": approx(0.7731, 0.001)},
        {"doc_id": 6, "text": "hello", "pmi": approx(0.0800, 0.001)},
    ]
    # g = build_inverted_index_graph('texts')
    # result = g.run(texts=rows)
    # split_words = Graph()
    # split_words.input('rows').map(tokenizer_mapper, doc_column=doc_column, text_column=text_column)

    # count_docs = Graph()
    # count_docs.input('rows').fold(folder=count_rows)

    # count_idf = Graph()
    # count_idf.input(split_words).sort(key=(doc_column, text_column))\
    #     .reduce(reducer=unique, key=(doc_column, text_column), doc_column=doc_column, text_column=text_column)\
    #     .join(count_docs, method='cross')\
    #     .sort(key=text_column)\
    #     .reduce(reducer=calc_idf, key=text_column, doc_column=doc_column, text_column=text_column)

    # calc_index = Graph()
    # calc_index.input(split_words).sort(key=doc_column)\
    #     .reduce(reducer=term_frequency_reducer, key=doc_column, doc_column=doc_column, text_column=text_column)\
    #     .join(count_idf, key=(text_column, text_column), method='left')\
    #     .sort(key=text_column)\
    #     .reduce(reducer=invert_index, key=text_column, doc_column=doc_column, text_column=text_column)

    split_words = Graph()
    split_words.input(input_stream).map(tokenizer_mapper, doc_column=doc_column, text_column=text_column)\
        .sort(key=doc_column)\
        .reduce(reducer=drop_less_then_two, key=doc_column, doc_column=doc_column, text_column=text_column)

    # result = split_words.run(rows=rows)
    # print(result)

    count_tf = Graph()
    count_tf.input(split_words).sort(key=doc_column)\
        .reduce(reducer=term_frequency_reducer, key=doc_column, doc_column=doc_column, text_column=text_column)

    # result = count_tf.run(rows=rows)
    # print(result)

    count_words = Graph()
    count_words.input(split_words).fold(folder=count_rows)

    count_tf_for_all = Graph()
    count_tf_for_all.input(split_words)\
        .join(count_words, method='cross')\
        .sort(key=text_column)\
        .reduce(reducer=tfr_for_all, key=text_column, text_column=text_column)

    # result = count_tf_for_all.run(rows=rows)
    # print(result)

    count_pmi = Graph()
    count_pmi.input(count_tf).join(count_tf_for_all, key=(text_column, text_column), method='left')\
        .sort(key=doc_column)\
        .reduce(reducer=pmi_counter, key=doc_column, doc_column=doc_column, text_column=text_column)

    count_pmi.run(rows=rows, output='out.txt')
    # print(result == etalon)


if __name__ == "__main__":
    main()
