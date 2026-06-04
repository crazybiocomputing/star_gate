# tests/test_star_gate.py
import io
import pandas as pd
from pathlib import Path
import pytest
import star_gate as sg

# Minimal, valid STAR 
# Data from wikipedia https://en.wikipedia.org/wiki/Solar_System
mock_star_data = """
data_planets
#
loop_
_planet
_period_days  # Orbital period (days)  
_period_years # Orbital period (Earth years)
_velocity     # Orbital velocity
Mercury     87.969   0.241  '47.9 km/s'
Venus      224.701   0.615  '35.0 km/s'
Earth      365.256   1.000  '29.8 km/s'
#
"""

D3 = [
    ['Mercury',     87.969,   0.241,  '47.9 km/s'],
    ['Venus',      224.701,   0.615,  '35.0 km/s'],
    ['Earth',      365.256,   1.000,  '29.8 km/s']
]

L = [
    ['Mars',     686.980,   1.881,  '24.1 km/s'],
    ['Jupiter',  4332.589,  11.862,  '13.1 km/s'],
    ['Saturn',   10759.22,   29.457,  '9.7 km/s'],
    ['Uranus' ,  30688.5,    84.020,  '6.8 km/s'],
    ['Neptune',  60182.0,   164.8,    '5.4 km/s'],
]

D = [
    {'planet': 'Mars',     'period_days': 686.980,   'period_years': 1.881,   'velocity': '24.1 km/s'},
    {'planet': 'Jupiter',  'period_days': 4332.589,  'period_years': 11.862,  'velocity': '13.1 km/s'},
    {'planet': 'Saturn',   'period_days': 10759.22,  'period_years': 29.457,  'velocity': '9.7 km/s'},
    {'planet': 'Uranus' ,  'period_days': 30688.5,   'period_years': 84.020,  'velocity': '6.8 km/s'},
    {'planet': 'Neptune',  'period_days': 60182.0,   'period_years': 164.8,   'velocity': '5.4 km/s'},
]

def test_read_column():
    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()

    pd.testing.assert_series_equal(
        table().row(1),
        pd.Series(
            ['Venus', 224.701, 0.615, '35.0 km/s'],
            index=['name','field','year'],
            name=1
        )
    )

    assert db.table().loc(0,'name') == 'Jacques Dubochet'

def test_append_list_row():

    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()
    print(table.df.shape)
  
    # Test `append(..)`
    table.append(L[0])

    expected_df = pd.DataFrame(data=D3 + [L[0]],columns=['planet','period_days','period_years','velocity'])

    pd.testing.assert_frame_equal(starship.datablock('planets').table().df, expected_df, check_like = True)

def test_append_dict_row():

    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()
  
    # Test `append(..)`
    table.append(D[0])

    expected_df = pd.DataFrame(data=D3 + [L[0]],columns=['planet','period_days','period_years','velocity'])

    pd.testing.assert_frame_equal(starship.datablock('planets').table().df, expected_df, check_like = True)

def test_concat_list():
    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()
  
    # Test `concat(..)`
    table.concat(L)

    expected_df = pd.DataFrame(data=D3 + L,columns=['planet','period_days','period_years','velocity'])
    pd.testing.assert_frame_equal(starship.datablock('planets').table().df, expected_df, check_like = True)    

def test_concat_dict():
    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()
  
    # Test `concat(..)`
    table.concat(D)
    print(starship.datablock('planets').table().df)

    expected_df = pd.DataFrame(data=D3 + L,columns=['planet','period_days','period_years','velocity'])
    print(expected_df)
    pd.testing.assert_frame_equal(starship.datablock('planets').table().df, expected_df, check_like = True)    

def test_multiple_append():
    starship = sg.StarGate()
    starship.parse(mock_star_data)
    table = starship.datablock('planets').table()

    # Test `append(..)`
    table.append(L[0])
    table.append(L[0])
    table.append(L[0])
    table.append(L[0])

    print(table.df.drop_duplicates().reset_index(drop=True),table.df.shape)

    expected_df = pd.DataFrame(data=D3 + [L[0]],columns=['planet','period_days','period_years','velocity'])

    pd.testing.assert_frame_equal(starship.datablock('planets').table().df, expected_df, check_like = True)    