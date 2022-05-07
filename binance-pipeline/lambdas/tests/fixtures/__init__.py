#./lambdas/tests/fixtures/__init__.py

# reverse load with optional first date
configuration_1 = {
        "start_date": "2020-01-01",
        "reverse": True
        }

# reverse load
configuration_2 = {
        "reverse": True
        }

# start from trade_id=0 optional end_date
configuration_3 = {
        "end_date": "2020-01-01",
        "reverse": False
        }

# start from trade_id=0
configuration_4 = {
        "reverse": False
        }

# start date, optional end date
configuration_5 = {
        "start_date": "2020-01-01",
        "end_date": "2021-02-01",
        "reverse": False
        }

# start date
configuration_6 = {
        "start_date": "2020-01-01",
        "reverse": False
        }
