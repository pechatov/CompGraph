from collections import Counter
from graph.graph import Graph
from math import log
from pytest import approx
import string


def minlen_filter_mapper(r):
    """
       filters out values with len(data) < 10
    """
    if len(r['name']) > 3:
        yield r


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


def count_rows(state, row):
    if state is None:
        state = {'docs_count': 0}
    state['docs_count'] += 1

    return state


def counting_reducer(records):
    cnt = 0
    for x in records:
        cnt += 1
        word = x['text']

    yield {'count': cnt, 'text': word}


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


def _filter_punctuation(txt):
    p = set(string.punctuation)
    return "".join([c for c in txt if c not in p])


def build_word_count_graph(input):
    my_graph = Graph()
    my_graph.input(input).map(tokenizer_mapper).sort(key='text').reduce(
        counting_reducer, key='text').sort(key='count')

    return my_graph


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
            counter = r['docs_count']

    yield {
        text_column: word,
        'idf': log(counter / len(elements))
    }


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
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
    ]
    doc_column = 'doc_id'
    text_column = 'text'
    etalon = [
        {"text": "hello", "doc_id": 5, "tf_idf": approx(0.2703, 0.001)},
        {"text": "hello", "doc_id": 1, "tf_idf": approx(0.1351, 0.001)},
        {"text": "hello", "doc_id": 4, "tf_idf": approx(0.1013, 0.001)},
        {"text": "little", "doc_id": 2, "tf_idf": approx(0.4054, 0.001)},
        {"text": "little", "doc_id": 3, "tf_idf": approx(0.4054, 0.001)},
        {"text": "little", "doc_id": 4, "tf_idf": approx(0.2027, 0.001)},
        {"text": "world", "doc_id": 6, "tf_idf": approx(0.3243, 0.001)},
        {"text": "world", "doc_id": 1, "tf_idf": approx(0.1351, 0.001)},
        {"text": "world", "doc_id": 5, "tf_idf": approx(0.1351, 0.001)}
    ]

    # g = build_inverted_index_graph('texts')
    # result = g.run(texts=rows)
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

    result = calc_index.run(rows=rows, verbose=False)
    assert result == etalon
    # print(result)
    # for i in range(len(result)):
    #     print('{} --- {}'.format(result[i], etalon[i]))
    # for x in result:
    #     print(x)
    # calc_index.print_nodes()
    # for x in result:
    #     for j in x:
    #         print(j)
    # for x in result:
    #     print(x)
    # result = calc_index.run(rows=rows)
    # calc_index.print_nodes(rows=rows)
    # print(result)
    # print(etalon)


if __name__ == "__main__":
    main()
