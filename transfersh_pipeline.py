import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This is a generic pipeline example and demonstrates
# how to use the dlt REST client for extracting data from APIs.
# It showcases the use of authentication via bearer tokens and pagination.


@dlt.source
def github_api_source(api_secret_key: str = dlt.secrets.value):
    # print(f"api_secret_key={api_secret_key}")
    return github_api_resource(api_secret_key)


@dlt.resource(write_disposition="append")
def github_api_resource(
    api_secret_key: str = dlt.secrets.value,
    org: str = "dutchcoders",
    repository: str = "transfer.sh",
):
    # paginate issues and yield every page
    api_url = f"https://api.github.com/repos/{org}/{repository}/issues?state=open"
    for page in paginate(
        api_url,
        auth=BearerTokenAuth(api_secret_key),
        # Note: for more paginators please see:
        # https://dlthub.com/devel/general-usage/http/rest-client#paginators
        paginator=HeaderLinkPaginator(),
    ):
        yield page


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name='github_api',
        destination='duckdb',
        dataset_name='github_api_data',
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(github_api_source())

    # pretty print the information on data that was loaded
    print(load_info)
