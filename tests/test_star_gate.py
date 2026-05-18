# tests/test_star_gate.py
import io
from pathlib import Path
import pytest
from star_gate import StarGate

def test_stargate_initialization():
    # Arrange & Act
    gate = StarGate()
    
    # Assert
    assert gate is not None
    # assert gate.is_active == False


def test_parse_simple_star_file():
    # 1. Minimal, valid STAR string
    # fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample_galaxy.star")
    fixture_path = Path(__file__).parent / "fixtures" / "galaxy.star"
    cargo = StarGate()
    cargo.read(fixture_path)
    print(type(cargo.db))
    #Get datablock
    result = cargo.datablock('global')
    print(type(result),result.id)

    # 2. Assertions
    assert result.id == "global"
    assert result.get("gate_id") == "SG-1"
    assert result.get("galaxy") == "Milky Way"
    assert result.get("chevrons") == 7

def test_parse_simple_star_block():
    # 1. Minimal, valid STAR string
    mock_star_data = """
    data_global
    #
    _gate_id   "SG-1"
    _galaxy    "Milky Way"
    _chevrons  7
    """
    
    cargo = StarGate()
    cargo.parse(mock_star_data)
    #Get datablock
    result = cargo.datablock('global')
    
    # 2. Assertions
    assert result.id == "global"
    assert result.get("gate_id") == "SG-1"
    assert result.get("galaxy") == "Milky Way"
    assert result.get("chevrons") == 7