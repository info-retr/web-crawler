import re
import sys
import logging


# proj2 stuff
def tokenize(str: str):
    tokens: list = []
    for token in str.split():
        splitNonAlphaNum: list = list(filter(None, re.split(r'[^0-9a-zA-Z]+', token)))
        if splitNonAlphaNum:
            for word in splitNonAlphaNum:
                tokens.append(word)
    return tokens


# List<Token> tokenize(TextFilePath)
# Map<Token,Count> computeWordFrequencies(List<Token>)
# void print(Frequencies<Token, Count>)

# Complexity: O(n), where n is the size of input (linear)
# While there are nested loops, O(n) can be also thought of as O(m*p),
# where m is the number of lines in the input file (separated by newline chars),
# and p is the average number of tokens per line (separated by space chars)
# Just a different way of saying input size. More lines, or tokens per line still means input size grows.
# The innermost for loop growth directly correlates with tokens per line.
def tokenize_file(TextFilePath: str) -> list:
    try:
        file = open(TextFilePath, 'r')
        text: str = file.read().strip()
        lines: list = text.splitlines()
        tokens: list = []
        for line in lines:
            for token in line.split():
                splitNonAlphaNum: list = list(filter(None, re.split(r'[^0-9a-zA-Z]+', token)))
                if splitNonAlphaNum:
                    for word in splitNonAlphaNum:
                        tokens.append(word)
        file.close()
    except FileNotFoundError:
        print('Debug: file doens\'t exist')
        sys.exit()
    else:
        return tokens


# checklist:
# complexity: O(n) because no nested iterative loops
def compute_word_frequencies(tokens: list) -> dict:
    wordFreqs: dict = {}
    for token in tokens:
        token = token.lower()
        if token not in wordFreqs:
            wordFreqs[token] = 0
        wordFreqs[token] += 1
    # printWordFreqs(wordFreqs)
    wordFreqs = dict(sorted(wordFreqs.items()))
    return dict(sorted(wordFreqs.items(), key=lambda item: item[1], reverse=True))


# complexity: O(n) because no nested iterative loops
def printWordFreqs(wordFreqs: dict) -> None:
    for key in wordFreqs:
        print(key, '\t', wordFreqs[key])


# complexity: O(m*n), same as tokenize(), the for loops here are just for cmdline args
if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 1:
        print('Only 1 file please')
        sys.exit()
    for arg in args:
        if not arg.endswith('.txt'):
            print('Invalid filename argument')
            sys.exit()
    tokensPerFiles: dict = {}
    wordFreqPerFile: dict = {}
    for arg in args:
        tokensPerFiles[arg] = tokenize(arg)
        wordFreqPerFile[arg] = computeWordFrequencies(tokensPerFiles[arg])
        print('\nWord frequencies of ', arg, ':')
        printWordFreqs(wordFreqPerFile[arg])

# sources (same as partB):
# https://docs.python.org/3.6/library/sys.html
# https://docs.python.org/3/library/re.html
# https://www.youtube.com/watch?v=vUBQLlv7_FI
# https://www.youtube.com/watch?v=ih8LVLplXa0
# https://stackoverflow.com/questions/5627425/what-is-a-good-way-to-handle-exceptions-when-trying-to-read-a-file-in-python
# https://docs.python.org/3/library/stdtypes.html#str.splitlines
# https://blog.finxter.com/python-one-line-for-loop-a-simple-tutorial/
# https://stackoverflow.com/questions/6579496/using-print-statements-only-to-debug
# https://realpython.com/iterate-through-dictionary-python/
# https://realpython.com/sort-python-dictionary/
# https://www.w3schools.com/python/ref_string_isalnum.asp
# https://www.w3resource.com/python-exercises/dictionary/python-data-type-dictionary-exercise-1.php
# https://stackoverflow.com/questions/1276764/stripping-everything-but-alphanumeric-chars-from-a-string-in-python
# https://stackoverflow.com/questions/2197451/why-are-empty-strings-returned-in-split-results
# https://stackoverflow.com/questions/12985456/replace-all-non-alphanumeric-characters-in-a-string
# https://stackoverflow.com/questions/42315072/python-update-a-key-in-dict-if-it-doesnt-exist
# https://www.youtube.com/watch?v=daefaLgNkw0