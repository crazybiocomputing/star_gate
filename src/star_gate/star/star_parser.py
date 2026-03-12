import re
import pandas as pd
from .star_common import CIF

#####################::: P A R S E R :::#####################

#
# Parsers
#
def parseComment(tok,obj):
    ## Reset if CIF.TABLE
    if obj['_admin_state'] == CIF.TABLE:
        obj['_admin_next'] = [CIF.TOKEN,CIF.TABLE]
        obj['_admin_state'] = CIF.NONE
        obj['_admin_current'] = None


def parseEOL(tok,obj):
    obj_block = obj['_admin_datablock']
    ## Convert to dataframe if CIF.TABLE
    if obj['_admin_state'] == CIF.TABLE:
        tablename = obj['_admin_current']
        obj_block[tablename] = pd.DataFrame(obj_block[tablename]['rows'],
                                            columns=obj_block[tablename]['header'])
    # Reset 
    obj['_admin_next'] = [CIF.DATABLOCK,CIF.TOKEN,CIF.TABLE]
    obj['_admin_state'] = CIF.NONE
    obj['_admin_current'] = None
    return obj

def parseSeparator(tok,obj):
  # Reset 
  obj['_admin_next'] = [CIF.STRING,CIF.NUMBER,CIF.TOKEN,CIF.WORD,CIF.EOF,CIF.EOL]
  return obj


def setHeader(cat,attr,obj) :
  # print('HEADER',cat)
  obj_block = obj['_admin_datablock']
  if cat not in obj_block:
    # Create Table
    obj_block[cat] = {
      'header': [],
      'rows': [[]]
    }

  obj_block[cat]['header'].append(attr)
  obj['_admin_current'] = cat # Current Category to fill in
  obj['_admin_next'] = [CIF.TOKEN,CIF.NUMBER,CIF.STRING,CIF.WORD]
  return obj



def parseNothing(tok,obj):
    '''
    * Parse nothing - Use for skipping token(s)
    *
    * @param {Token} - `tok` a token composed of 
    * @param {Object} - `obj` the Data Structure
    * @returns {object} - The UNmodified Data Structure
    *
    * @author Jean-Christophe Taveau
    '''
    #
    # do nothing
    #print('Nothing')
    pass

def setCategory(cat,attr,obj):
    '''
    * Set mmCIF Category
    *
    * @param {Token} - `tok` a token composed of 
    * @param {Object} - `obj` the Data Structure
    * @returns {object} - The modified Data Structure
    *
    * @author Jean-Christophe Taveau
    '''
    
    obj_block = obj['_admin_datablock']
    ## Create object if needed
    if cat not in list(obj_block.keys()):
        obj_block[cat] = {}
    
    if attr == None:
        obj_block[cat] = 0
    else:
        obj_block[cat][attr] = 0
        
    obj['_admin_next'] = [CIF.NUMBER,CIF.STRING,CIF.WORD]
    obj['_admin_current'] = cat
    obj['_admin_token'] = attr
    return obj



def parseToken(tok,obj):
    '''
        * Parse mmCIF Token: parseHead or parseCat depending of context (CIF.TABLE or CIF.TOKEN)
        *
        * @param {Object} - `tok` a token composed of a type `type` and a value `v`.
        * @param {Object} - `obj` the Data Structure.
        * @returns {object} - The modified Data Structure.
        *
        * @author Jean-Christophe Taveau
    '''
  
    ## Remove the leading underscore `_` and Split `category` and `attribute` 
    array = tok['v'][1:].split('.')
    cat = ''
    attr = ''
    if len(array) == 1 and obj['_admin_state'] == CIF.TABLE: 
        cat = 'table'
        attr = array[0]
    elif len(array) == 1 and obj['_admin_state'] != CIF.TABLE: 
        cat = array[0]
        attr = None
    else:
        cat = array[0]
        attr = array[1]
    # print('TOKEN',cat,attr,obj['_admin_state'] )
    return setHeader(cat,attr,obj) if obj['_admin_state'] == CIF.TABLE else setCategory(cat,attr,obj)



def setRowValue(tok,obj):
    '''
        Parse mmCIF Value in Token or in Table: setRowValue or parseCat depending of context (CIF.TABLE or CIF.TOKEN)
        
        @param {Object} - `tok` a token composed of a type `type` and a value `v`.
        @param {Object} - `obj` the Data Structure.
        @returns {object} - The modified Data Structure.
        
        @author Jean-Christophe Taveau
    '''
    # print('ROW_VALUE',tok['v'])
    obj_block = obj['_admin_datablock']
    
    table = obj_block[obj['_admin_current']]
    last = len(table['rows']) - 1
    if len(table['rows'][last]) >= len(table['header']): 
        table['rows'].append([]) # Add a new row in the table
        last += 1 # Update
    
    table['rows'][last].append(tok['v'])
    obj['_admin_next'] = [CIF.STRING,CIF.WORD,CIF.NUMBER, CIF.EOL, CIF.SEPARATOR]
    return obj


def setTokenValue(tok,obj):
    # print('VALUE',tok['v'])
    obj_block = obj['_admin_datablock']
    # print(obj['_admin_current'],obj['_admin_token'],obj['_admin_next'],tok)
    if obj['_admin_token'] == None:
        obj_block[obj['_admin_current']] = tok['v']
    else:
        obj_block[obj['_admin_current']][obj['_admin_token']] = tok['v']
    obj['_admin_current'] = None
    obj['_admin_token']= None
    obj['_admin_next'] = [CIF.STRING,CIF.WORD,CIF.NUMBER, CIF.EOL,CIF.SEPARATOR]
    return obj


def parseValue(tok,obj):
    return setRowValue(tok,obj) if obj['_admin_state'] == CIF.TABLE else setTokenValue(tok,obj)



def parseTable(tok,obj):
    '''
    * Parse mmCIF Table
    *
    * @param {Object} - `tok` a token composed of a type `type` and a value `v`.
    * @param {Object} - `obj` the Data Structure.
    * @returns {object} - The modified Data Structure.
    *
    * @author Jean-Christophe Taveau
    '''
    obj['_admin_state'] = CIF.TABLE
    obj['_admin_next'] = [CIF.TOKEN]
    # print(obj['_admin_state'],'parseTable',tok)
    return obj


def parseDataBlock(tok,obj):
    '''
    * Parse mmCIF DataBlock
    *
    * @param {Object} - `tok` a token composed of a type `type` and a value `v`.
    * @param {Object} - `obj` the Data Structure.
    * @returns {object} - The modified Data Structure.
    *
    * @author Jean-Christophe Taveau
    *
    '''
    
    # print('new DB',tok['v'])
    id = tok['v'] if len(tok['v']) > 1 else 'default'
    obj['_admin_next'] = [CIF.TOKEN,CIF.TABLE] # TOKEN, TABLE
    db = {'id': id}
    obj['datablocks'][id] = db # .append(db)
    obj['_admin_datablock'] = db
    obj['_admin_current'] = None
    obj['_admin_token'] = None
    
    return obj



def parser(toks):
    '''
        * Parsers for the following tokens:
        *  - NONE: 0 ,
        *  - SEPARATOR: 1,
        *  - COMMENT: 2,
        *  - DATABLOCK: 3,
        *  - TOKEN: 4,
        *  - TABLE: 5,
        *  - HEADER: 6,
        *  - STRING: 7,
        *  - WORD: 8,
        *  - NUMBER: 9
        *  - EOL: 10
    '''
    
    indexes = [CIF.NONE,CIF.SEPARATOR,CIF.COMMENT,
               CIF.DATABLOCK,CIF.TOKEN,CIF.TABLE,
               CIF.HEADER,CIF.STRING,CIF.WORD,CIF.NUMBER,CIF.EOL,CIF.EOF]
    setters = [
        parseNothing,parseSeparator,parseComment,
        parseDataBlock,parseToken,parseTable,
        parseNothing,parseValue,parseValue,parseValue,parseEOL,parseEOL
    ]
    
    model = {
        '_admin_next': [CIF.DATABLOCK],
        '_admin_state': CIF.NONE,
        'datablocks': {}
    }
    
    # print(toks)
    for tok in toks:
        if tok['type'] in model['_admin_next']:
            idx = indexes.index(tok['type'])
            # print(model)
            setters[idx](tok,model)
      
    # print(tok,model['datablocks'])
    
    # Delete _admin_*
    del model['_admin_next']
    del model['_admin_state']
    del model['_admin_current']
    del model['_admin_token']
    del model['_admin_datablock']
    
    return model['datablocks']

