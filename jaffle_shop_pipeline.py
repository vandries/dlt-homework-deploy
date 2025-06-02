import os
import dlt
from threading import currentThread
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

os.environ['BASE_URL'] = "https://jaffle-shop.scalevector.ai/api/v1"

os.environ['PAGE_SIZE'] = "5000"

os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = "5000"

os.environ['EXTRACT__DATA_WRITER__FILE_MAX_ITEMS'] = "5000"
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = "1000"

os.environ['EXTRACT__WORKERS'] = "5"
os.environ['NORMALIZE__WORKERS'] = "1"
os.environ['LOAD__WORKERS'] = "5"

def jaffle_source():
    # Simple REST Client
    client = RESTClient(
        base_url=os.environ['BASE_URL'],
        paginator=PageNumberPaginator(base_page=1, page_param="page", total_path=None)
    )

    # Customers resource
    @dlt.resource(table_name="customers", write_disposition="replace", parallelized=True)
    def customers_resource():
        params = {
            'page_size': os.environ['PAGE_SIZE']
        }
        yield client.paginate("/customers", params=params)
    
    # Orders resource
    @dlt.resource(table_name="orders", write_disposition="replace", parallelized=True)
    def orders_resource():
        params = {
            'page_size': os.environ['PAGE_SIZE']
        }
        yield client.paginate("/orders", params=params)
    
    # Products resource
    @dlt.resource(table_name="products", write_disposition="replace", parallelized=True)
    def products_resource():
        params = {
            'page_size': os.environ['PAGE_SIZE']
        }
        yield client.paginate("/products", params=params)

    return customers_resource, orders_resource, products_resource

if __name__ == "__main__":
    # Init Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_pipeline",
        destination="duckdb",
        dataset_name="raw_data",
    )

    # Run pipeline
    load_info = pipeline.run(jaffle_source())
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
