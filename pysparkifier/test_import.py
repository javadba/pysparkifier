from .pysparkifier import *

def test_getSpark():
  spark1 = setupSpark()
  spark2 = setupSpark()
  assert spark1==spark2, "Why different?"
  
def test_stay_private():
  import pytest
  with pytest.raises(NameError):
    keep_me_private()

    