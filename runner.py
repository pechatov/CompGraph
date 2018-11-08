from collections import Counter
from graph.graph import Graph


def minlen_filter_mapper(r):
    """
       filters out values with len(data) < 10
    """
    if len(r['text']) > 3:
        yield r


def tokenizer_mapper(r):
    """
       splits rows with 'text' field into set of rows with 'token' field
      (one for every occurence of every word in text)
    """

    tokens = r['text'].split()

    for token in tokens:
        yield {
            'doc_id': r['doc_id'],
            'text': token,
        }


def sum_columns_folder(state, record):
    for column in state:
        state[column] += record[column]
    return state


def term_frequency_reducer(records):
    '''
        calculates term frequency for every word in doc_id
    '''

    word_count = Counter()

    for r in records:
        word_count[r['text']] += 1

    total = sum(word_count.values())
    for w, count in word_count.items():
        yield {
            'doc_id': r['doc_id'],
            'text': w,
            'tf': count / total
        }


def main():
    g = Graph()
    f = Graph()

    f.map(tokenizer_mapper).map(minlen_filter_mapper)

    g.input(f).sort(
        key=('doc_id')).reduce(term_frequency_reducer, 'doc_id')  # .fold(folder, state={'doc_id': 0, 'text': 0})
    docs = [
        {'doc_id': 1, 'text': 'hello hello hello my little WORLD absdasc'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'},
        {'doc_id': 3, 'text': 'ABScsa Hello, my aaaaaabs little little helldasd'}
    ]
    # g.run(docs)
    # for x in g._queue[-1].result:
    #     print(x)
    # print('*' * 100)

    names = [
        {'name': 'Andrey', 'id': 1}, {'name': 'leonid', 'id': 2},
        {'name': 'Sergey', 'id': 1}, {'name': 'Georgy', 'id': 4}
    ]

    cities = [
        {'city_name': 'Moscow', 'city_id': 1},
        {'city_name': 'Kazan', 'city_id': 3},
        {'city_name': 'StPt', 'city_id': 2},
        {'city_name': 'Voronezh', 'city_id': 2}
    ]
    q = Graph()
    w = Graph()
    # print(q._queue)
    # print(w._queue)
    # w.run(cities)
    q.join(w, key=('id', 'city_id'), method='inner')
    # print(q._queue)
    q._queue[1].input = cities
    # print(q._queue[-1].input, q._queue[-1].second_input)
    q.run(names)
    # for node in q._queue:
    #     print(node.result)

    # print(type(q))
    # print(q._queue[-1].result)
    for i in q._queue[-1].result:
        print(i)
    # print(w._queue[-1].result)

if __name__ == "__main__":
    main()
