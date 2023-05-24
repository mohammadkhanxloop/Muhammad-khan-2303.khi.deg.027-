from my_proj.sum_func import add, add_positive

# Importing necessary functions


# Checking addition of 3 and 4 equals 7
def test_add_3_4_returns_7():
    assert add(3, 4) == 7

# Checking addition of positive numbers 3 and 4 equals 7
def test_add_positive_3_4_returns_7():
    assert add_positive(3, 4) == 7

# Checking addition of positive 3 and negative -4 returns None
def test_add_positive_3_neg4_returns_None():
    assert add_positive(3, -4) == None

# Checking addition of negative -3 and positive 4 returns None
def test_add_positive_neg_3_4_returns_None():
    assert add_positive(-3, 4) == None
