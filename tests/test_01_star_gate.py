# tests/test_star_gate.py
import io
import pandas as pd
from pathlib import Path
import pytest
from star_gate import StarGate

def test_stargate_initialization():
    # Arrange & Act
    starship = StarGate()
    
    # Assert
    assert starship is not None
    # assert gate.is_active ==p False

def test_parse_simple_star_block():
    # 1. Minimal, valid STAR string
    mock_star_data = """
    data_global
    #
    _gate_id   "SG-1"
    _galaxy    "Milky Way"
    _chevrons  7
    """
    
    starship = StarGate()
    starship.parse(mock_star_data)

    # 2. Get datablock
    db = starship.datablock('global')
    
    # 3. Assertions
    assert db.id == "global"
    assert db.get("gate_id") == "SG-1"
    assert db.get("galaxy") == "Milky Way"
    assert db.get("chevrons") == 7

def test_stargate_empty_table():
    # 1. Minimal, valid STAR string
    mock_star_data = """
    data_cryoem
    #
    loop_
    _name
    _field
    _year
    #
    """
    
    starship = StarGate()
    starship.parse(mock_star_data)
    print(starship.datablock('cryoem').table())

def test_parse_simple_star_table_column():
    # 1. Minimal, valid STAR string
    mock_star_data = """
    data_cryoem
    #
    loop_
    _name
    _field
    _year
    "Jacques Dubochet"   chemistry 2017
    'Joachim Frank'      chemistry 2017
    "Richard Henderson"  chemistry 2017
    #
    """
    
    starship = StarGate()
    starship.parse(mock_star_data)

    # 2. Get datablock
    db = starship.datablock('cryoem')
    
    # 3. Assertions
    assert db.id == "cryoem"
    pd.testing.assert_series_equal(
        db.table().column('name'),
        pd.Series(['Jacques Dubochet','Joachim Frank','Richard Henderson'],name='name')
    )
    pd.testing.assert_series_equal(
        db.table().column('field'),
        pd.Series(['chemistry','chemistry','chemistry'],name='field')
    )


def test_parse_simple_star_keyvalue_table():
    # 1. Minimal, valid STAR string
    # dedent multiline string
    mock_star_data = """
    data_cryoem
    #
    _scientific_field chemistry
    _nobel_year       2017
    #
    loop_
    _first_name
    _name
    Jacques "Dubochet"   
    Joachim 'Frank'      
    Richard "Henderson" 
    #
    """

    starship = StarGate()
    starship.parse(mock_star_data)

    # 2. Get datablock
    db = starship.datablock('cryoem')
    # 3. Assertions
    expected_dict = {
        'db_id': 'cryoem',
        'scientific_field': 'chemistry',
        'nobel_year': 2017.0,
        'table': {
            'rows': [
                ['Jacques', 'Dubochet' ],
                ['Joachim', 'Frank'    ],
                ['Richard', 'Henderson']
            ],
            'columns': ['first_name','name']
        }
    }
    expected_df =  pd.DataFrame(
        [
            ['cryoem', 'Jacques', 'Dubochet',  'chemistry', 2017.0],
            ['cryoem', 'Joachim', 'Frank',     'chemistry', 2017.0],
            ['cryoem', 'Richard', 'Henderson', 'chemistry', 2017.0]
        ],
        columns = ['db_id','first_name','name','scientific_field','nobel_year']
    )
    print(db.to_star(),starship.db,starship.save('test.star'))
    assert db.db_id == "cryoem"
    assert db.to_star() == expected_dict
    pd.testing.assert_frame_equal(db.df, expected_df, check_like = True)

def test_parse_simple_star_file():
    # 1. Load STAR file
    # fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample_galaxy.star")
    file_path = Path(__file__).parent / "fixtures" / "galaxy.star"
    starship = StarGate()
    starship.read(file_path)

    # 2. Get datablock
    db = starship.datablock('global')

    # 3. Assertions
    assert db.id == "global"
    assert db.get("gate_id") == "SG-1"
    assert db.get("galaxy") == "Milky Way"
    assert db.get("chevrons") == 7

