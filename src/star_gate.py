import numpy as np
import pandas as pd
from .star.star_common import CIF
from .star.star_tokenizer import tokenize
from .star.star_parser import parser

           
class Block:
    def __init__(self,dic):
        self.db = dic
    
    def id(self):
        return self.db['id']
    
    def table(self):
        if 'table' in self.db.keys():
            return Table(self.db['table'])
        return None
    
    def value_of(self,category,attr='value'):
        return self.db[category][attr]

class Table:
    def __init__(self,dic):
        self.table = dic
    
    def headers(self):
        return self.table['header']
    
    def rows(self):
        return self.table['rows']

    def dataframe(self,colindex=0):
        # creating DataFrame
        df = pd.DataFrame(self.table['rows'], columns=self.table['header'])
        df.set_index(self.table['header'][colindex],inplace=True,drop=False)
        return df
        
    def row(self,i):
        return self.table['rows'][i]
    
    def column(self,headname):
        i = self.headers().index(headname)
        col = []
        for row in self.table['rows']:
          col.append(row[i])
        return col
    
    
class StarGate:
    def __init__(self):
        self.db = {}

    @property
    def blocks(self):
        return self.db
        
    def read(self,filename):
        with open(filename) as f:
            self.parseSTAR(f.read()) 
    
    def save(self,blocks,filename):
        """
            Save blocks as Dictionary containing key/value and/or table
            block = {
                'key': value,
                'table' : {
                    'rows|data': [..],
                    'columns' : [..]
                }
            }
        """
        with open(filename,'a') as f:
            for blockid in blocks.keys():
                txt = self._block_to_string(blocks[blockid],blockid)
                f.write(txt)
    
    def save_tables(self,blocks,filename):
        """
            Save blocks as Dictionary of Dataframes
            block =  pd.DataFrame
        """
        with open(filename,'a') as f:
            for blockid in blocks.keys():
                txt = self._dataframe_to_string(blocks[blockid],blockid)
                f.write(txt)
    
    def datablock(self,blockname):
        for db in self.db['datablocks']:
            if db['id'] == blockname:
                return Block(db)
        return None
    
    def table_of(self,blockname):
        for db in self.db['datablocks']:
            if db['id'] == blockname and db['table']:
                return db
        return None
    
    def parseSTAR(self,txt):
        ## First Pass
        tokens = tokenize(txt);
        ## Second Pass - Parse
        self.db = parser(tokens);


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
        msg = f'data_{blockid}\n\n'
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


    
