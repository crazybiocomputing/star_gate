
from enum import Enum

## Constants for state
class CIF(Enum):
  NONE= 0
  SEPARATOR= 1
  COMMENT= 2
  DATABLOCK= 3
  TOKEN= 4
  TABLE= 5
  HEADER= 6
  STRING= 7
  WORD= 8
  NUMBER= 9
  EOL= 10
  EOF= 11
