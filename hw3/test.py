import re, string

def extract_words(s):
    '''return word list from doc'''
    if s is None: return ['']
    s = re.sub(r'[^\x00-\x7f]',r'', s)
    s = s.translate(str.maketrans('', '', string.punctuation))
    return s.lower().split()

def build_shingles(x, k):
    '''return shingle set from doc'''
    s = x[1]
    ls = list(set([tuple(s[i:(i+k)]) for i in range(len(s) - k + 1)]))
    return list(map(lambda e: (e, x[0]), ls))

def build_shingle_vector(x, num_doc):
    '''
    return row shingle vector (shingle, [0,0,1, ...])
    if shingle exist in doc set 1 else 0
    '''
    vector = [0 for _ in range(num_doc)]
    shingle, doc_idxs = x

    for i in doc_idxs:
        vector[i] = 1

    return (shingle, vector)
   

def test_extract_words():
    text = 'this is taipei city'
    output = ['this', 'is', 'taipei', 'city']
    print(extract_words(text))
    assert extract_words(text) == output

test_extract_words()


def test_build_shingles():
    text = 'this is taipei city'
    k = 2
    output = [(('this', 'is'), 0), (('is', 'taipei'), 0), (('taipei', 'city'), 0)]
    print(build_shingles((0, extract_words(text)), k))

    assert build_shingles(text, k) == output

test_build_shingles()
