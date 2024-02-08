if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer

def transform(data, *args, **kwargs):
    # Replace spaces with underscores and convert to lowercase for column names
    data.columns = data.columns.str.replace(' ', '_').str.lower()

    # Rename the first column to 'id'
   # data.rename(columns={data.columns[0]: 'id'}, inplace=True)

    return data






