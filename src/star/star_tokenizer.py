
#####################::: T O K E N I Z E R :::#####################

import re
from .star_common import CIF

###### Predicates ######

def isDataBlock(w):
  return w[0:5] == 'data_'
  
def isTable(w):
  return w[0:5] == 'loop_'
  
def isFirst(symbol,w):
  return (w[0] == symbol)

def isComment(w):
  return isFirst('#',w)
  
def isToken(w):
  return isFirst('_',w)
  
def isMultiLine(w):
  return isFirst(';',w)

def isString(w):
  return isFirst('\'',w);

def isNumber(w):
  try:
    float_value = float(w)
  except ValueError:
    return False
    
  return True
   
def isEOL(w):
  return re.search('\n\n+',w)

def isSeparator(w):
  #return w.split('').every( ch => [' ','\t'].includes(ch)) || w.split('').filter(ch => ch == '\n').length == 1;
  return re.search(r'([ \t]+\n?|\n)',w)
  
def isWord(w):
  return True

###### Token Creation ######

## Create Basic Token
def basicToken(typ):
  def func(w,i,array):
    return [{'type': typ,'v':w},i]
  return func
  
## Create Numeric Token
def numericToken (typ):
  def func(w,i,array):
    return [{'type': typ,'v':float(w)},i];
  return func
  
## Create StringToken
def appendWord(predicate,array,j,str):
    # Remove first delimiter
    new_str = ''
    word = array[j][1:]

    while predicate(word):
        new_str += word
        j += 1
        word = array[j]
    # Add last word and clean
    new_str += word
    new_str = new_str.replace('\n','')
    return [j,new_str] 


def stringToken(typ,predicate):
    def func(w,i,array) :
        [j,s] = appendWord(predicate,array,i,w);
        ## Remove leading delimiters
        v = s[:-1]
        return [{'type': typ,'v': v},j];
        
    return func

keywords = [
  {
    'predicate': isDataBlock,
    'newToken': lambda w,i,array: [{'type': CIF.DATABLOCK,'v':w[5:]},i],
  },
  {
    'predicate': isTable,
    'newToken': basicToken(CIF.TABLE)
  },
  {
    'predicate': isComment,
    'newToken': stringToken(CIF.COMMENT,lambda word: False if word == '' else word[0] != '\n')
  },
  {
    'predicate': isEOL,
    'newToken': basicToken(CIF.EOL) 
  },
  {
    'predicate': isSeparator,
    'newToken': basicToken(CIF.SEPARATOR) 
  },
  {
    'predicate': isToken,
    'newToken': basicToken(CIF.TOKEN)
  },
  {
    'predicate': isMultiLine,
    'newToken': stringToken(CIF.STRING, lambda word: word[0] != ';')
  },
  {
    'predicate': isNumber,
    'newToken': numericToken(CIF.NUMBER)
  },
  {
    'predicate': isString,
    'newToken': stringToken(CIF.STRING, lambda word: word[-1] != '\'')
  },
  {
    'predicate': isWord,
    'newToken': basicToken(CIF.WORD) 
  }
]

def setToken(words,index):
  w = words[index]
  ## Get Token corresponding to keyword
  toks = []
  for kw in keywords:
    if kw['predicate'](w):
      toks.append(kw['newToken'](w,index,words));
  
  # Add new Token. Only the first one because the other(s) are less prioritary. The last one is always `CIF.WORD`
  return toks[0]; # keyword.newToken(w,index,words);

def tokenize(txt):
  '''
    mmCIF Tokenizer
  '''
  
  # Remove comments
  clean = re.sub('#.*\n','\n',txt)
  # Split
  words = re.split(r'(\s+)',clean)

  tokens = [];
  index = 0;

  ## TODO Use (tail) recursion
  while index < len(words):
    if len(words[index]) != 0:
      [tok,index] = setToken(words,index);
      tokens.append(tok);
    index += 1

  tokens.append({'type': CIF.EOF, 'v': ''})
  # print(tokens);
  return tokens;

