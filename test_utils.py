from utils import is_prime

def test_is_prime():
    assert is_prime(5) is True
    assert is_prime(2) is True
    assert is_prime(7) is True
    assert is_prime(4) is False
    assert is_prime(6) is False
    assert is_prime(8) is False
    