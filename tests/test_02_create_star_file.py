# tests/test_star_gate.py
import io
import pandas as pd
from pathlib import Path
import pytest
import star_gate as sg


def test_stargate_initialization():
    starship = sg.StarGate()
    # Datablock `global`
    blck = sg.Block('global')
    # Add three key/value pairs
    blck.set('gate_id',"SG-1")
    blck.set('galaxy',"Milky Way")
    blck.set('chevrons',7)
    starship.add(blck)

    # 2. Get datablock
    db = starship.datablock('global')
    print(type(db),db.id)
    print(db.df)
    # 3. Assertions
    expected = pd.DataFrame(
        {
            'db_id': 'global',
            'db_type': 'star',
            'gate_id' :"SG-1",
            'galaxy'  : "Milky Way",
            'chevrons': 7
        },
        index = [0]
    )
    assert db.id == "global"
    assert db.get("gate_id") == "SG-1"
    assert db.get("galaxy") == "Milky Way"
    assert db.get("chevrons") == 7
    pd.testing.assert_frame_equal(db.df, expected)

    db.set("planet","saturn")
    print(db.df['planet'])
    print(pd.Series(['saturn']))
    assert db.get('planet') == 'saturn'
    pd.testing.assert_series_equal(db.df['planet'],pd.Series(['saturn'],name='planet'))
