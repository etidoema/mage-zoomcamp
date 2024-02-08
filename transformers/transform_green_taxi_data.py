if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

# Step  1: Remove rows where passenger count is  0 or trip distance is  0
    data = data[(data['passenger_count'] !=  0) & (data['trip_distance'] !=  0)] 

# Step  2: Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

# Step  3: Rename columns in CamelCase to snake_case
    data.rename(columns=lambda x: x.lower().replace(" ", "_"), inplace=True)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['passenger_count'] >   0).all(), "passenger_count is not greater than   0"
    assert (output['trip_distance'] >   0).all(), "trip_distance is not greater than   0"
    assert 'vendorid' in output.columns, 'There are not vendor_id column'


    
