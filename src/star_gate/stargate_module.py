import numpy as np
import pandas as pd
from .star.star_common import CIF
from .star.star_tokenizer import tokenize
from .star.star_parser import parser

        
class Block:
    def __init__(self,data,block_type='star'):
        """
        Empty STAR|mmCIF Datablock
        type: 'star' or 'cif'
        """
        if block_type in ['star','cif']:
            self.type = block_type if block_type == 'star' else 'cif'
        self.db = data
        self._id = data['id']
    
    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self,blockname):
        self._id = blockname
    
    def get(self,key):
        if key in self.db.keys():
            return self.db[key]
        else:
            return None

    def set(self,key,value):
        self.db[key] = value
            
    def add(self,tdata,tname = 'table'):
        if self.type == 'star' and tname != 'table':
            return 'ERROR: A table in a STAR file cannot have a name'
        self.db[tname] = tdata

    def table(self,tname = 'table'):
        if 'table' in self.db.keys():
            return Table(self.db[tname])
        return None
    
    def dataframe(self):
        if self.type == 'star':
            # Merge key/values and table in one single dataframe
            # Each key corresponds to a column of a constant (`value`) 
            if 'table' in self.db.keys():
                # Step #1 get the table
                df = self.table()
                for key,value in self.db.items():
                    df.loc[:, key] = value
            else:
                df = pd.DataFrame(self.db.values(),columns=self.db.keys())
            return df
    
    def value_of(self,category,attr='value'):
        return self.db[category][attr]
    
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        s = f'data_{self.id}\n'
        for k in self.db.keys():
            if k == 'table':
                s += str(self.db['table'])
            else:
                s+= f'{k} {self.db[k]}\n'
        return s

class Table:
    def __init__(self,data=None,columns=None):
        self.df = pd.DataFrame(data=data, columns=columns)
    
    def from_dict(self,dict):
        self.df = pd.DataFrame(dict)

    def columns(self):
        return self.df.columns
    
    def rows(self):
        return self.df.data

    def dataframe(self,colindex=0):
        # creating DataFrame
        df = pd.DataFrame(self.table['data'], columns=self.table['columns'])
        df.set_index(self.table['columns'][colindex],inplace=True,drop=False)
        return df
        
    def row(self,i):
        return self.table['data'][i]
    
    def column(self,headname):
        i = self.headers().index(headname)
        col = []
        for row in self.table['data']:
          col.append(row[i])
        return col
    
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        s = '#\nloop_\n'
        for h in self.df.columns:
            s += f'{h}\n'
        for row in self.df.data:
            s += ' '.join([d for d in row]) + '\n'
        s += '#\n'
        return s
    
class StarGate:
    def __init__(self):
        self.db = {}

    @property
    def blocks(self):
        return self.db
    
    def add(self,block):
        self.db[block.id] = block

    def read(self,filename):
        with open(filename) as f:
            self.parseSTAR(f.read()) 
    
    def parse(self,data):
        self.parseSTAR(data) 
    
    def save(self,filename,orient='split'):
        """
        Save blocks as Dictionary containing key/value and/or table
        block = {
            'key': value,
            'table' : {
                'rows|data': [..],
                'columns' : [..]
            }
        }
        orient : Determines the type of the values of the dictionary.
        'dict' (default) : dict like {column -> {index -> value}}
        'list' : dict like {column -> [values]}
        'series' : dict like {column -> Series(values)}
        'split' : dict like {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
        'tight' : dict like {'index' -> [index], 'columns' -> [columns], 'data' -> [values], 'index_names' -> [index.names], 'column_names' -> [column.names]}
        'records' : list like [{column -> value}, … , {column -> value}]
        'index' : dict like {index -> {column -> value}}

        """
        if orient in ['dict', 'list', 'series', 'split', 'tight', 'records', 'index']:
            with open(filename,'a') as f:
                for blockid in self.db.keys():
                    txt = self._block_to_string(self.db[blockid],blockid)
                    f.write(txt)
        return 'Unknown mode'
    
    def save_tables(self,blocks,filename):
        """
            Save blocks as Dictionary of Dataframes
            block =  pd.DataFrame
        """
        with open(filename,'a') as f:
            for blockid in blocks.keys():
                txt = self._dataframe_to_string(blocks[blockid],blockid)
                f.write(txt)
    
    def datablock(self,blockname=None):
        if blockname is None:
            b = next(iter(self.db))
            return Block(b)
        # if blockname set
        for bn in self.db.keys():
            if bn == blockname:
                return Block(self.db[bn])
        return None
    
    def table_of(self,blockname):
        for db in self.db['datablocks']:
            if db['id'] == blockname and db['table']:
                return db
        return None
    
    def parseSTAR(self,txt):
        ## First Pass
        tokens = tokenize(txt)
        ## Second Pass - Parse
        self.db = parser(tokens)

    def to_json(self):
        """Build JSON object from StarGate datablocks"""
        d = self.db # Datablocks
        for k, v in d.items():
            if isinstance(v, pd.DataFrame):
                d[k] = v.T.to_dict(orient='split')
            elif type(v) == dict:
                d[k] = self.to_json(v)
        return d
    
    def to_object(self,mmcif=False):
        def obj_dic(d):
            top = type('new', (object,), d)
            seqs = (tuple, list, set, frozenset)
            for i, j in d.items():
                if isinstance(j, dict):
                    setattr(top, i, obj_dic(j))
                elif isinstance(j, seqs):
                    setattr(top, i, 
                        type(j)(obj_dic(sj) if isinstance(sj, dict) else sj for sj in j))
                else:
                    setattr(top, i, j)
            return top
       
        # Main
        if mmcif:
            id = next(iter(self.db))
            return obj_dic(self.db[id])
        else:
            return obj_dic(self.db)

    def _block_to_string(self,block,blockid):
        # Create input star file
        msg = f'data_{blockid}\n#\n'
        for key in block.keys():
            if key == 'table':
                msg +='loop_\n'
                for i,k in enumerate(block['table']['columns']):
                    spc = ' ' * (30 - len(k))
                    msg += f'_spr{k}{spc}#{i+1}\n'
                # Check if key `data` from dataframe.to_dict()
                rows = 'data' if 'data' in block['table'] else 'rows'
                for row in block['table'][rows]:
                    for v in row:
                        if type(v) == str and len(v.split()) > 1:
                            msg += f'\'{v}\'  '
                        else:
                            msg += f'{v}  '
                    msg += '\n'
                msg += '\n#\n'
            else:
                spc = ' ' * (30 - len(key))
                if type(block[key]) == str and len(block[key].split()) > 1:
                    msg += f'_{key}{spc}\'{block[key]}\'\n'
                else:
                    msg += f'_{key}{spc}{block[key]}\n'
        msg += f'\n# End of datablock {blockid}\n\n'
        return msg
        
    def _dataframe_to_string(self,block,blockid):
        # Create input star file
        msg = f'data_{blockid}\n\n'
        msg +='loop_\n'
        # Write headings
        for i,k in enumerate(block.columns):
                spc = ' ' * (30 - len(k))
                msg += f'_spr{k}{spc}#{i+1}\n'
        # Write rows
        for i in range(block.shape[0]):
            row = block.iloc[i, :].values.flatten().tolist()
            for v in row:
                if type(v) == str and len(v.split()) > 1:
                    msg += f'\'{v}\'  '
                else:
                    msg += f'{v}  ' 
            msg += '\n'
        msg += '#\n'

        msg += f'\n# End of datablock {blockid}\n\n'
        return msg

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        s = ''
        for blck in self.db.values():
            s += str(blck)
        return s
    
